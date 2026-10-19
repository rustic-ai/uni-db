//! Program generator — the single source of truth for differential cases.
//!
//! Each builder emits, from one set of parameters, the triple consumed by the
//! differential test: the Cypher that seeds the base graph, the equivalent Locy
//! program text (run by the engine), and the oracle IR (run by this crate's
//! evaluator). Because the same parameters drive all three, the engine and
//! oracle never share evaluation code, only inputs.
//!
//! The program text is held as a [`SurfaceProgram`] (see [`crate::surface`])
//! and rendered from it, so a test can rename or reorder it.
//!
//! # Binding families
//!
//! [`random_program_strategy`] also draws a [`BindingShape`], which adds rules
//! aimed at how a clause binds its variables (issue #300): clauses with two
//! or more `IS` references, subjects bound by a reference rather than by the
//! `MATCH`, body variables named like the referenced rules' columns, value
//! columns two references share, `ALONG prev.q` beside another reference
//! exposing `q`, scalar KEY columns whose integers equal live VIDs, and
//! `IS NOT` written before or after its binder. Each family's conjuncts are
//! written in a random order.

// Rust guideline compliant

use std::collections::{BTreeSet, HashMap};
use std::ops::Range;

use proptest::prelude::*;

use crate::eval::{OracleError, check_program};
use crate::ir::{
    Agg, ColKind, ColumnRead, Computed, Feature, Filter, FoldAgg, Generated, IsRef, OracleBest,
    OracleClause, OracleFold, OracleProgram, OracleRule, PROB_SCALE, ProbComplement, ProbFactor,
    RuleColumn, Tuple, WireKind,
};
use crate::ir::{CmpOp, Operand};
use crate::surface::{
    Conjunct, Piece, SurfaceClause, SurfaceProgram, SurfaceRef, SurfaceStmt, Tmpl, YieldItem,
    clause, filter, is_not_ref, is_ref, item, key_var, own_col, value_var,
};

/// Closed-form transitive-closure cardinality of [`build_layered_dag`].
///
/// In a layered DAG with `stages` fully-bipartite layers of `width` nodes, every
/// node in layer `L` reaches every node in each later layer `L' > L`. So the
/// closure has `width²` pairs for each of the `C(stages, 2)` ordered layer pairs:
/// `width² · stages · (stages − 1) / 2`.
///
/// # Examples
/// ```
/// use uni_locy_oracle::generator::expected_closure_size;
/// assert_eq!(expected_closure_size(4, 15), 1350); // the historical IS-NOT-bug shape
/// assert_eq!(expected_closure_size(1, 9), 0); // a single layer has no edges
/// ```
#[must_use]
pub fn expected_closure_size(stages: usize, width: usize) -> usize {
    width * width * stages * stages.saturating_sub(1) / 2
}

/// Edges of a layered fully-bipartite DAG, as oracle base tuples `[src_id, dst_id]`.
///
/// Node ids are `layer * width + index`. Consecutive layers are fully connected:
/// every node in layer `L` has an `EDGE` to every node in layer `L + 1`.
fn layered_dag_edges(stages: usize, width: usize) -> Vec<Tuple> {
    let mut edges = Vec::new();
    for layer in 0..stages.saturating_sub(1) {
        for i in 0..width {
            for j in 0..width {
                let src = (layer * width + i) as i64;
                let dst = ((layer + 1) * width + j) as i64;
                edges.push(vec![src, dst]);
            }
        }
    }
    edges
}

/// Renders the base-graph Cypher: one `CREATE` binding every node, then every edge.
///
/// Uses only basic `CREATE` with bound variables (`n{id}`) — the most widely
/// supported Cypher — so seeding never depends on `UNWIND`/`range`/list features.
/// Each `(edge_type, edges)` group is emitted under its own relationship type.
fn render_cypher(node_count: usize, edge_groups: &[(&str, &[Tuple])]) -> String {
    let total_edges: usize = edge_groups.iter().map(|(_, e)| e.len()).sum();
    let mut parts: Vec<String> = Vec::with_capacity(node_count + total_edges);
    for id in 0..node_count {
        parts.push(format!("(n{id}:Node {{id: {id}}})"));
    }
    for (edge_type, edges) in edge_groups {
        for e in *edges {
            parts.push(format!("(n{})-[:{edge_type}]->(n{})", e[0], e[1]));
        }
    }
    format!("CREATE {}", parts.join(", "))
}

/// Builds a `var -> column` binding map from `(name, index)` pairs.
fn var_cols(pairs: &[(&str, usize)]) -> HashMap<String, usize> {
    pairs.iter().map(|(v, i)| ((*v).to_string(), *i)).collect()
}

/// Rule columns from `(name, kind)` pairs, KEY columns first.
fn columns(spec: &[(&str, ColKind)]) -> Vec<RuleColumn> {
    spec.iter().map(|(n, k)| RuleColumn::new(*n, *k)).collect()
}

/// The two `reaches` clauses (base + recursive transitive closure) over `EDGE`.
fn reaches_surface() -> Vec<SurfaceStmt> {
    vec![
        clause(
            "reaches",
            "({a}:Node)-[:EDGE]->({b}:Node)",
            Vec::new(),
            "",
            vec![key_var("a"), key_var("b")],
        ),
        clause(
            "reaches",
            "({a}:Node)-[:EDGE]->({mid}:Node)",
            vec![is_ref(&["mid"], "reaches", Some("b"))],
            "",
            vec![key_var("a"), key_var("b")],
        ),
    ]
}

/// Assembles a [`Generated`] from its surface and oracle faces, deriving the
/// program text, the per-relation schemas and wire kinds, and the features
/// of the `family` rules.
fn finish(
    base_graph_cypher: String,
    surface: SurfaceProgram,
    strata: Vec<Vec<OracleRule>>,
    strings: HashMap<String, i64>,
    family: &BTreeSet<String>,
) -> Generated {
    let oracle_rules = OracleProgram { strata };
    let mut key_schema = HashMap::new();
    let mut col_kinds = HashMap::new();
    for rule in oracle_rules.strata.iter().flatten() {
        key_schema.insert(
            rule.name.clone(),
            rule.columns.iter().map(|c| c.name.clone()).collect(),
        );
        let mut kinds: Vec<WireKind> = rule
            .columns
            .iter()
            .map(|c| match c.kind {
                ColKind::Node => WireKind::Node,
                ColKind::Int => WireKind::Int,
                ColKind::Str => WireKind::Str,
                ColKind::Prob => WireKind::Prob,
            })
            .collect();
        // A SUM is a float by definition, whatever its inputs.
        if let Some(fold) = &rule.fold {
            for (i, a) in fold.aggs.iter().enumerate() {
                if a.agg == Agg::Sum {
                    kinds[fold.key_count + i] = WireKind::IntegralFloat;
                }
            }
        }
        col_kinds.insert(rule.name.clone(), kinds);
    }
    let rel_features = derive_features(&surface, &oracle_rules, family);
    Generated {
        base_graph_cypher,
        program_text: surface.canonical_text(),
        surface,
        oracle_rules,
        key_schema,
        col_kinds,
        strings,
        features: rel_features.values().flatten().copied().collect(),
        rel_features,
    }
}

/// The `reaches` rule IR: `reaches(a,b) :- EDGE(a,b)` ∪ `EDGE(a,mid), reaches(mid,b)`.
fn reaches_rule(edges: &[Tuple]) -> OracleRule {
    OracleRule {
        per_path: false,
        best: None,
        fold: None,
        name: "reaches".to_string(),
        columns: columns(&[("a", ColKind::Node), ("b", ColKind::Node)]),
        key_count: 2,
        clauses: vec![
            OracleClause {
                computed: Vec::new(),
                prob_complements: Vec::new(),
                base: edges.to_vec(),
                var_cols: var_cols(&[("a", 0), ("b", 1)]),
                pos_refs: Vec::new(),
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "b".to_string()],
                ..OracleClause::default()
            },
            OracleClause {
                computed: Vec::new(),
                prob_complements: Vec::new(),
                base: edges.to_vec(),
                var_cols: var_cols(&[("a", 0), ("mid", 1)]),
                pos_refs: vec![IsRef {
                    values: Vec::new(),
                    rule: "reaches".to_string(),
                    subjects: vec!["mid".to_string()],
                    target: Some("b".to_string()),
                }],
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "b".to_string()],
                ..OracleClause::default()
            },
        ],
    }
}

/// All ordered pairs of node ids `[a, b]` — the `MATCH (a:Node), (b:Node)` relation.
fn all_pairs(node_count: usize) -> Vec<Tuple> {
    let n = node_count as i64;
    (0..n)
        .flat_map(|a| (0..n).map(move |b| vec![a, b]))
        .collect()
}

/// Builds a transitive-closure program over a layered fully-bipartite DAG.
///
/// This is the workhorse family for the threshold-straddling property test: the
/// closure size [`expected_closure_size(stages, width)`](expected_closure_size)
/// can be tuned to land on either side of the engine's 300-fact dedup-strategy
/// switch. The recursive rule is right-recursive transitive closure:
///
/// ```text
/// CREATE RULE reaches AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
/// CREATE RULE reaches AS MATCH (a:Node)-[:EDGE]->(mid:Node)
///   WHERE mid IS reaches TO b YIELD KEY a, KEY b
/// ```
///
/// Degenerate inputs are handled, not rejected: `stages < 2` or `width == 0`
/// yields an edgeless graph and an empty closure.
#[must_use]
pub fn build_layered_dag(stages: usize, width: usize) -> Generated {
    let node_count = stages * width;
    let edges = layered_dag_edges(stages, width);

    finish(
        render_cypher(node_count, &[("EDGE", &edges)]),
        SurfaceProgram {
            stmts: reaches_surface(),
        },
        vec![vec![reaches_rule(&edges)]],
        HashMap::new(),
        &BTreeSet::new(),
    )
}

/// Builds a program whose second stratum is the stratified `IS NOT` complement
/// of the transitive closure: `unreached(a,b) :- Node(a), Node(b), NOT reaches(a,b)`.
///
/// This is the headline family — it exercises the anti-join path that carried the
/// historical `IS NOT` complement bug. Choose `stages`/`width` so the closure
/// exceeds 300 (e.g. `(4, 12)` → 864) to drive the over-threshold dedup strategy;
/// the oracle catches a divergence regardless of that trigger.
///
/// The closed-form complement size is `(stages·width)² − expected_closure_size`.
/// Keep `stages · width` modest (≤ ~80) so the all-pairs base stays tractable.
#[must_use]
pub fn build_complement(stages: usize, width: usize) -> Generated {
    let node_count = stages * width;
    let edges = layered_dag_edges(stages, width);

    let mut stmts = reaches_surface();
    stmts.push(clause(
        "unreached",
        "({a}:Node), ({b}:Node)",
        vec![is_not_ref(&["a"], "reaches", Some("b"), false)],
        "",
        vec![key_var("a"), key_var("b")],
    ));

    // unreached(a, b) :- all-pairs(a, b), NOT reaches(a, b). Negation keys on the
    // full (a, b) tuple, so subjects carries both bound variables.
    let unreached = OracleRule {
        per_path: false,
        best: None,
        fold: None,
        name: "unreached".to_string(),
        columns: columns(&[("a", ColKind::Node), ("b", ColKind::Node)]),
        key_count: 2,
        clauses: vec![OracleClause {
            computed: Vec::new(),
            prob_complements: Vec::new(),
            base: all_pairs(node_count),
            var_cols: var_cols(&[("a", 0), ("b", 1)]),
            pos_refs: Vec::new(),
            neg_refs: vec![IsRef {
                values: Vec::new(),
                rule: "reaches".to_string(),
                subjects: vec!["a".to_string(), "b".to_string()],
                target: None,
            }],
            yield_vars: vec!["a".to_string(), "b".to_string()],
            ..OracleClause::default()
        }],
    };

    finish(
        render_cypher(node_count, &[("EDGE", &edges)]),
        SurfaceProgram { stmts },
        vec![vec![reaches_rule(&edges)], vec![unreached]],
        HashMap::new(),
        &BTreeSet::new(),
    )
}

/// Builds a non-recursive union: `linked` is the same head from two base
/// patterns, `EDGE` and its reverse `EDGE2`.
///
/// Exercises multi-clause union semantics — the derived relation must equal the
/// set union of the two edge relations. `EDGE` and `EDGE2` are disjoint (forward
/// vs. backward), so the union has exactly `2 · |EDGE|` pairs.
#[must_use]
pub fn build_union(stages: usize, width: usize) -> Generated {
    let node_count = stages * width;
    let forward = layered_dag_edges(stages, width);
    let reverse: Vec<Tuple> = forward.iter().map(|e| vec![e[1], e[0]]).collect();

    let stmts = ["EDGE", "EDGE2"]
        .map(|ty| {
            clause(
                "linked",
                &format!("({{a}}:Node)-[:{ty}]->({{b}}:Node)"),
                Vec::new(),
                "",
                vec![key_var("a"), key_var("b")],
            )
        })
        .to_vec();

    let oracle_clause = |base: Vec<Tuple>| OracleClause {
        computed: Vec::new(),
        prob_complements: Vec::new(),
        base,
        var_cols: var_cols(&[("a", 0), ("b", 1)]),
        pos_refs: Vec::new(),
        neg_refs: Vec::new(),
        yield_vars: vec!["a".to_string(), "b".to_string()],
        ..OracleClause::default()
    };

    finish(
        render_cypher(node_count, &[("EDGE", &forward), ("EDGE2", &reverse)]),
        SurfaceProgram { stmts },
        vec![vec![OracleRule {
            per_path: false,
            best: None,
            fold: None,
            name: "linked".to_string(),
            columns: columns(&[("a", ColKind::Node), ("b", ColKind::Node)]),
            key_count: 2,
            clauses: vec![oracle_clause(forward), oracle_clause(reverse)],
        }]],
        HashMap::new(),
        &BTreeSet::new(),
    )
}

/// A proptest strategy yielding [`build_layered_dag`] instances over given bounds.
///
/// Exposed from the library (rather than inlined in the test) so downstream test
/// crates can reuse the same generator. Pick `width`/`stages` ranges whose
/// closure size straddles the 300-fact threshold.
///
/// # Examples
/// ```
/// use uni_locy_oracle::generator::layered_dag_strategy;
/// let _strategy = layered_dag_strategy(2..6, 5..30);
/// ```
pub fn layered_dag_strategy(
    stages: Range<usize>,
    width: Range<usize>,
) -> impl Strategy<Value = Generated> {
    (stages, width).prop_map(|(s, w)| build_layered_dag(s, w))
}

// ---------------------------------------------------------------------------
// Random programs over a multigraph
// ---------------------------------------------------------------------------

/// One generated edge: `(src id, dst id, weight, second type?)`.
pub type RandomEdge = (i64, i64, i64, bool);

/// Which binding families a random program adds (see the
/// [module docs](self#binding-families)), and the entropy that draws their
/// names, constants and conjunct orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingShape {
    /// The families, in the order of [`BindingShape::FAMILIES`].
    pub families: [bool; 7],
    /// Seeds the draws inside the families.
    pub entropy: u64,
}

impl BindingShape {
    /// The family names, index for index with [`families`](Self::families).
    pub const FAMILIES: [&'static str; 7] = [
        "multi-ref KEY capture (P1)",
        "reference-bound subjects",
        "capture-seeking names",
        "shared value names",
        "prev beside a value column",
        "scalar KEYs",
        "IS NOT before and after its binder",
    ];

    /// No binding family: the legacy program.
    #[must_use]
    pub fn none() -> Self {
        Self {
            families: [false; 7],
            entropy: 0,
        }
    }
}

/// The shape a random program takes, drawn by [`random_program_strategy`].
#[derive(Debug, Clone)]
pub struct RandomShape {
    /// Node count; ids are `0..nodes`.
    pub nodes: usize,
    /// Edges, parallel edges and self-loops included.
    pub edges: Vec<RandomEdge>,
    /// `link` (and so `reach`) follows edges backwards.
    pub reverse: bool,
    /// `link` also follows the second edge type.
    pub union: bool,
    /// Emit `un` = node pairs `reach` does not relate.
    pub negation: bool,
    /// Which FOLD rules to emit: `deg`, `span`, `flow`.
    pub folds: [bool; 3],
    /// Emit `cost` (per-path `ALONG`) and `tot` (a FOLD over its paths).
    pub along: bool,
    /// Emit `roll`, a recursive FOLD that reads its children's folded value.
    pub rollup: bool,
    /// Emit `best`: `ALONG` with `BEST BY` — `Some(true)` the shortest path
    /// over every edge (cycles included), `Some(false)` the longest over the
    /// acyclic edges.
    pub best: Option<bool>,
    /// Emit the PROB rules: `pr` (a PROB column), `nor` / `prd` (MNOR / MPROD
    /// over edges) and `safe` (a probabilistic `IS NOT nor`).
    pub prob: bool,
    /// The binding families to add.
    pub binding: BindingShape,
    /// The recursion families to add.
    pub recursion: RecursionShape,
}

/// The reader of family 1 (`o IS seed, (o, e, x) IS wt`): issue #300's P1.
pub const P1_READER: &str = "p1";
/// The integer scalar-KEY reader of family 6 (Q1-A): `a IS tgi TO col`.
pub const SCALAR_INT_READER: &str = "ski";
/// The string scalar-KEY reader of family 6.
pub const SCALAR_STR_READER: &str = "sks";
/// The `ALONG` rule of family 5 (Q2): `prev.q` beside `a IS dsc`.
pub const PREV_RULE: &str = "pth";

/// Random programs over a random multigraph.
///
/// The graph has self-loops, parallel edges (identical ones included) and two
/// relationship types with integer weights. Every node carries its `id`, an
/// integer `k = (id + 1) % nodes` (seeded ids equal VIDs, so every `k` is the
/// VID of another node) and a string `name`. The program stacks, in strata:
///
/// * `link(a, b)`: an edge, forward or backward, of the first type or both;
/// * `reach(a, b)`: the transitive closure of the same edges (a set);
/// * `un(a, b)`: node pairs `reach` does not relate (stratified `IS NOT`);
/// * FOLD rules, each over a **bag** of rows: `deg` over the first type's edges
///   (`COUNT(*)`, `SUM`, `MIN`, `MAX`, `COUNT`), `span` over `reach`
///   (`COUNT(*)`, `MAX`, `MMIN`), and `flow` over an edge joined with `reach`
///   (`COUNT(*)`, `MSUM`, `MCOUNT`);
/// * the binding families of a [`BindingShape`].
/// * the recursion families of a [`RecursionShape`]: non-linear and mutually
///   recursive strata with `BEST BY`, `FOLD`, `ALONG` and `IS NOT`.
///
/// # Examples
/// ```
/// use uni_locy_oracle::generator::random_program_strategy;
/// let _strategy = random_program_strategy();
/// ```
pub fn random_program_strategy() -> impl Strategy<Value = Generated> {
    (2usize..9)
        .prop_flat_map(|nodes| {
            let n = nodes as i64;
            (
                Just(nodes),
                proptest::collection::vec((0..n, 0..n, 1i64..6, any::<bool>()), 0..20),
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
                any::<[bool; 3]>(),
                // `along` is weighted up: `cost` is non-empty only when the
                // graph also has an ascending first-type edge, and at even
                // odds it was non-empty in 36% of cases, which missed its 20%
                // coverage floor in about one 96-case run in 3,000. At 0.65
                // it is 48%, and a miss is below one in a million.
                (proptest::bool::weighted(0.65), any::<bool>(), any::<bool>()),
                proptest::option::of(any::<bool>()),
                (any::<[bool; 7]>(), any::<u64>()),
                (
                    any::<[bool; 6]>(),
                    proptest::option::of(any::<bool>()),
                    any::<u64>(),
                ),
            )
        })
        .prop_map(
            |(
                nodes,
                edges,
                reverse,
                union,
                negation,
                folds,
                (along, rollup, prob),
                best,
                (families, entropy),
                (
                    [
                        nonlinear,
                        mutual,
                        mutual_best,
                        mutual_fold,
                        mutual_along,
                        rec_negation,
                    ],
                    nonlinear_best,
                    rec_entropy,
                ),
            )| {
                build_random_program(&RandomShape {
                    nodes,
                    edges,
                    reverse,
                    union,
                    negation,
                    folds,
                    along,
                    rollup,
                    best,
                    prob,
                    binding: BindingShape { families, entropy },
                    recursion: RecursionShape {
                        nonlinear,
                        nonlinear_best,
                        mutual,
                        mutual_best,
                        mutual_fold,
                        mutual_along,
                        negation: rec_negation,
                        entropy: rec_entropy,
                    },
                })
            },
        )
}

/// The string property `name` of node `id`.
fn node_name(id: i64) -> String {
    format!("s{id}")
}

/// Builds the program, graph and oracle IR for one [`RandomShape`].
#[must_use]
pub fn build_random_program(shape: &RandomShape) -> Generated {
    // Base tuples: [src, dst, edge index, weight], per relationship type. The
    // edge index keeps parallel edges distinct rows of the bag.
    let typed = |second: bool| -> Vec<Tuple> {
        shape
            .edges
            .iter()
            .enumerate()
            .filter(|(_, e)| e.3 == second)
            .map(|(i, e)| vec![e.0, e.1, i as i64, e.2])
            .collect()
    };
    let (first, second) = (typed(false), typed(true));
    let (a_col, b_col) = if shape.reverse { (1, 0) } else { (0, 1) };
    let arrow = |ty: &str| {
        if shape.reverse {
            format!("<-[:{ty}]-")
        } else {
            format!("-[:{ty}]->")
        }
    };
    let mut kinds: Vec<(&str, &Vec<Tuple>)> = vec![("EDGE", &first)];
    if shape.union {
        kinds.push(("EDGE2", &second));
    }

    let mut stmts: Vec<SurfaceStmt> = Vec::new();
    let ab = || vec!["a".to_string(), "b".to_string()];

    // link
    let mut link = OracleRule {
        per_path: false,
        best: None,
        fold: None,
        name: "link".to_string(),
        columns: columns(&[("a", ColKind::Node), ("b", ColKind::Node)]),
        key_count: 2,
        clauses: Vec::new(),
    };
    for (ty, base) in &kinds {
        stmts.push(clause(
            "link",
            &format!("({{a}}:Node){}({{b}}:Node)", arrow(ty)),
            Vec::new(),
            "",
            vec![key_var("a"), key_var("b")],
        ));
        link.clauses.push(OracleClause {
            computed: Vec::new(),
            prob_complements: Vec::new(),
            base: (*base).clone(),
            var_cols: var_cols(&[("a", a_col), ("b", b_col)]),
            pos_refs: Vec::new(),
            neg_refs: Vec::new(),
            yield_vars: ab(),
            ..OracleClause::default()
        });
    }

    // reach
    let mut reach = OracleRule {
        per_path: false,
        best: None,
        fold: None,
        name: "reach".to_string(),
        columns: columns(&[("a", ColKind::Node), ("b", ColKind::Node)]),
        key_count: 2,
        clauses: Vec::new(),
    };
    for (ty, base) in &kinds {
        stmts.push(clause(
            "reach",
            &format!("({{a}}:Node){}({{b}}:Node)", arrow(ty)),
            Vec::new(),
            "",
            vec![key_var("a"), key_var("b")],
        ));
        stmts.push(clause(
            "reach",
            &format!("({{a}}:Node){}({{m}}:Node)", arrow(ty)),
            vec![is_ref(&["m"], "reach", Some("b"))],
            "",
            vec![key_var("a"), key_var("b")],
        ));
        reach.clauses.push(OracleClause {
            computed: Vec::new(),
            prob_complements: Vec::new(),
            base: (*base).clone(),
            var_cols: var_cols(&[("a", a_col), ("b", b_col)]),
            pos_refs: Vec::new(),
            neg_refs: Vec::new(),
            yield_vars: ab(),
            ..OracleClause::default()
        });
        reach.clauses.push(OracleClause {
            computed: Vec::new(),
            prob_complements: Vec::new(),
            base: (*base).clone(),
            var_cols: var_cols(&[("a", a_col), ("m", b_col)]),
            pos_refs: vec![IsRef {
                values: Vec::new(),
                rule: "reach".to_string(),
                subjects: vec!["m".to_string()],
                target: Some("b".to_string()),
            }],
            neg_refs: Vec::new(),
            yield_vars: ab(),
            ..OracleClause::default()
        });
    }

    let mut strata = vec![vec![link], vec![reach]];

    if shape.negation {
        stmts.push(clause(
            "un",
            "({a}:Node), ({b}:Node)",
            vec![is_not_ref(&["a"], "reach", Some("b"), false)],
            "",
            vec![key_var("a"), key_var("b")],
        ));
        strata.push(vec![OracleRule {
            per_path: false,
            best: None,
            fold: None,
            name: "un".to_string(),
            columns: columns(&[("a", ColKind::Node), ("b", ColKind::Node)]),
            key_count: 2,
            clauses: vec![OracleClause {
                computed: Vec::new(),
                prob_complements: Vec::new(),
                base: all_pairs(shape.nodes),
                var_cols: var_cols(&[("a", 0), ("b", 1)]),
                pos_refs: Vec::new(),
                neg_refs: vec![IsRef {
                    values: Vec::new(),
                    rule: "reach".to_string(),
                    subjects: ab(),
                    target: None,
                }],
                yield_vars: ab(),
                ..OracleClause::default()
            }],
        }]);
    }

    let agg = |agg: Agg, input: Option<usize>| FoldAgg { agg, input };
    let mut folds: Vec<OracleRule> = Vec::new();

    // The acyclic part of the first type: edges to a greater id. Per-path
    // values and a recursive MSUM only converge without cycles.
    let dag: Vec<Tuple> = first.iter().filter(|e| e[0] < e[1]).cloned().collect();
    let all_nodes: Vec<Tuple> = (0..shape.nodes as i64).map(|id| vec![id]).collect();
    let computed = |name: &str, vars: &[&str], constant: i64| Computed {
        name: name.to_string(),
        vars: vars.iter().map(|v| (*v).to_string()).collect(),
        factor: 1,
        constant,
    };
    // `<rule>(a, b, v)`: an edge's weight, or an edge followed by a fact of the
    // rule itself, adding the weight to its value (`prev.v + e.w`).
    let path_clauses = |rule: &str, base: &Vec<Tuple>| {
        vec![
            OracleClause {
                base: base.clone(),
                var_cols: var_cols(&[("a", 0), ("b", 1), ("w", 3)]),
                pos_refs: Vec::new(),
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "b".to_string(), "v".to_string()],
                computed: vec![computed("v", &["w"], 0)],
                prob_complements: Vec::new(),
                ..OracleClause::default()
            },
            OracleClause {
                base: base.clone(),
                var_cols: var_cols(&[("a", 0), ("m", 1), ("w", 3)]),
                pos_refs: vec![IsRef {
                    rule: rule.to_string(),
                    subjects: vec!["m".to_string()],
                    target: Some("b".to_string()),
                    values: vec!["p".to_string()],
                }],
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "b".to_string(), "v".to_string()],
                computed: vec![computed("v", &["p", "w"], 0)],
                prob_complements: Vec::new(),
                ..OracleClause::default()
            },
        ]
    };

    if shape.along {
        stmts.push(clause(
            "cost",
            "({a}:Node)-[{e}:EDGE]->({b}:Node)",
            vec![filter("{a}.id < {b}.id")],
            "ALONG {cost:q} = {e}.w",
            vec![key_var("a"), key_var("b"), own_col("cost", "q")],
        ));
        stmts.push(clause(
            "cost",
            "({a}:Node)-[{e}:EDGE]->({m}:Node)",
            vec![filter("{a}.id < {m}.id"), is_ref(&["m"], "cost", Some("b"))],
            "ALONG {cost:q} = prev.{cost:q} + {e}.w",
            vec![key_var("a"), key_var("b"), own_col("cost", "q")],
        ));
        strata.push(vec![OracleRule {
            name: "cost".to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("b", ColKind::Node),
                ("q", ColKind::Int),
            ]),
            key_count: 2,
            clauses: path_clauses("cost", &dag),
            fold: None,
            per_path: true,
            best: None,
        }]);

        stmts.push(clause(
            "tot",
            "({a}:Node)",
            vec![is_ref(&["a"], "cost", Some("b"))],
            "FOLD {tot:n} = COUNT(*), {tot:s} = SUM({cost:q})",
            vec![key_var("a"), own_col("tot", "n"), own_col("tot", "s")],
        ));
        folds.push(OracleRule {
            name: "tot".to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("n", ColKind::Int),
                ("s", ColKind::Int),
            ]),
            key_count: 1,
            clauses: vec![OracleClause {
                base: all_nodes.clone(),
                var_cols: var_cols(&[("a", 0)]),
                pos_refs: vec![IsRef {
                    rule: "cost".to_string(),
                    subjects: vec!["a".to_string()],
                    target: Some("b".to_string()),
                    values: vec!["q".to_string()],
                }],
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "q".to_string()],
                computed: Vec::new(),
                prob_complements: Vec::new(),
                ..OracleClause::default()
            }],
            fold: Some(OracleFold {
                key_count: 1,
                aggs: vec![agg(Agg::CountStar, None), agg(Agg::Sum, Some(1))],
            }),
            per_path: false,
            best: None,
        });
    }

    if shape.rollup {
        stmts.push(clause(
            "roll",
            "({a}:Node)",
            Vec::new(),
            "",
            vec![key_var("a"), item(false, "1", "s")],
        ));
        stmts.push(clause(
            "roll",
            "({a}:Node)-[{e}:EDGE]->({c}:Node)",
            vec![filter("{a}.id < {c}.id"), is_ref(&["c"], "roll", None)],
            "FOLD {roll:s} = MSUM({roll:s} + {e}.w)",
            vec![key_var("a"), own_col("roll", "s")],
        ));
        strata.push(vec![OracleRule {
            name: "roll".to_string(),
            columns: columns(&[("a", ColKind::Node), ("s", ColKind::Int)]),
            key_count: 1,
            clauses: vec![
                OracleClause {
                    base: all_nodes.clone(),
                    var_cols: var_cols(&[("a", 0)]),
                    pos_refs: Vec::new(),
                    neg_refs: Vec::new(),
                    yield_vars: vec!["a".to_string(), "s".to_string()],
                    computed: vec![computed("s", &[], 1)],
                    prob_complements: Vec::new(),
                    ..OracleClause::default()
                },
                OracleClause {
                    base: dag.clone(),
                    var_cols: var_cols(&[("a", 0), ("c", 1), ("w", 3)]),
                    pos_refs: vec![IsRef {
                        rule: "roll".to_string(),
                        subjects: vec!["c".to_string()],
                        target: None,
                        values: vec!["cs".to_string()],
                    }],
                    neg_refs: Vec::new(),
                    yield_vars: vec!["a".to_string(), "x".to_string()],
                    computed: vec![computed("x", &["cs", "w"], 0)],
                    prob_complements: Vec::new(),
                    ..OracleClause::default()
                },
            ],
            fold: Some(OracleFold {
                key_count: 1,
                aggs: vec![agg(Agg::Sum, Some(1))],
            }),
            per_path: false,
            best: None,
        }]);
    }

    if let Some(ascending) = shape.best {
        let (order, base) = if ascending {
            ("ASC", &first)
        } else {
            ("DESC", &dag)
        };
        let acyclic = |a: &str, b: &str| -> Vec<Conjunct> {
            if ascending {
                Vec::new()
            } else {
                vec![filter(&format!("{{{a}}}.id < {{{b}}}.id"))]
            }
        };
        stmts.push(clause(
            "opt",
            "({a}:Node)-[{e}:EDGE]->({b}:Node)",
            acyclic("a", "b"),
            &format!("ALONG {{opt:d}} = {{e}}.w BEST BY {{opt:d}} {order}"),
            vec![key_var("a"), key_var("b"), own_col("opt", "d")],
        ));
        let mut recursive = acyclic("a", "m");
        recursive.push(is_ref(&["m"], "opt", Some("b")));
        stmts.push(clause(
            "opt",
            "({a}:Node)-[{e}:EDGE]->({m}:Node)",
            recursive,
            &format!("ALONG {{opt:d}} = prev.{{opt:d}} + {{e}}.w BEST BY {{opt:d}} {order}"),
            vec![key_var("a"), key_var("b"), own_col("opt", "d")],
        ));
        strata.push(vec![OracleRule {
            name: "opt".to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("b", ColKind::Node),
                ("d", ColKind::Int),
            ]),
            key_count: 2,
            clauses: path_clauses("opt", base),
            fold: None,
            per_path: false,
            best: Some(OracleBest {
                key_count: 2,
                criterion: 2,
                ascending,
            }),
        }]);
    }
    if shape.folds[0] {
        stmts.push(clause(
            "deg",
            "({a}:Node)-[{e}:EDGE]->({b}:Node)",
            Vec::new(),
            "FOLD {deg:n} = COUNT(*), {deg:s} = SUM({e}.w), {deg:lo} = MIN({e}.w), \
             {deg:hi} = MAX({e}.w), {deg:c} = COUNT({e}.w)",
            vec![
                key_var("a"),
                own_col("deg", "n"),
                own_col("deg", "s"),
                own_col("deg", "lo"),
                own_col("deg", "hi"),
                own_col("deg", "c"),
            ],
        ));
        folds.push(OracleRule {
            per_path: false,
            best: None,
            name: "deg".to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("n", ColKind::Int),
                ("s", ColKind::Int),
                ("lo", ColKind::Int),
                ("hi", ColKind::Int),
                ("c", ColKind::Int),
            ]),
            key_count: 1,
            clauses: vec![OracleClause {
                computed: Vec::new(),
                prob_complements: Vec::new(),
                base: first.clone(),
                var_cols: var_cols(&[("a", 0), ("w", 3)]),
                pos_refs: Vec::new(),
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "w".to_string()],
                ..OracleClause::default()
            }],
            fold: Some(OracleFold {
                key_count: 1,
                aggs: vec![
                    agg(Agg::CountStar, None),
                    agg(Agg::Sum, Some(1)),
                    agg(Agg::Min, Some(1)),
                    agg(Agg::Max, Some(1)),
                    agg(Agg::Count, Some(1)),
                ],
            }),
        });
    }
    if shape.folds[1] {
        stmts.push(clause(
            "span",
            "({a}:Node)",
            vec![is_ref(&["a"], "reach", Some("b"))],
            "FOLD {span:n} = COUNT(*), {span:hi} = MAX({b}.id), {span:lo} = MMIN({b}.id)",
            vec![
                key_var("a"),
                own_col("span", "n"),
                own_col("span", "hi"),
                own_col("span", "lo"),
            ],
        ));
        folds.push(OracleRule {
            per_path: false,
            best: None,
            name: "span".to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("n", ColKind::Int),
                ("hi", ColKind::Int),
                ("lo", ColKind::Int),
            ]),
            key_count: 1,
            clauses: vec![OracleClause {
                computed: Vec::new(),
                prob_complements: Vec::new(),
                base: (0..shape.nodes as i64).map(|id| vec![id]).collect(),
                var_cols: var_cols(&[("a", 0)]),
                pos_refs: vec![IsRef {
                    values: Vec::new(),
                    rule: "reach".to_string(),
                    subjects: vec!["a".to_string()],
                    target: Some("b".to_string()),
                }],
                neg_refs: Vec::new(),
                yield_vars: ab(),
                ..OracleClause::default()
            }],
            fold: Some(OracleFold {
                key_count: 1,
                aggs: vec![
                    agg(Agg::CountStar, None),
                    agg(Agg::Max, Some(1)),
                    agg(Agg::Min, Some(1)),
                ],
            }),
        });
    }
    if shape.folds[2] {
        stmts.push(clause(
            "flow",
            "({a}:Node)-[{e}:EDGE]->({m}:Node)",
            vec![is_ref(&["m"], "reach", Some("b"))],
            "FOLD {flow:n} = COUNT(*), {flow:s} = MSUM({e}.w), {flow:k} = MCOUNT({b}.id)",
            vec![
                key_var("a"),
                own_col("flow", "n"),
                own_col("flow", "s"),
                own_col("flow", "k"),
            ],
        ));
        folds.push(OracleRule {
            per_path: false,
            best: None,
            name: "flow".to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("n", ColKind::Int),
                ("s", ColKind::Int),
                ("k", ColKind::Int),
            ]),
            key_count: 1,
            clauses: vec![OracleClause {
                computed: Vec::new(),
                prob_complements: Vec::new(),
                base: first.clone(),
                var_cols: var_cols(&[("a", 0), ("m", 1), ("w", 3)]),
                pos_refs: vec![IsRef {
                    values: Vec::new(),
                    rule: "reach".to_string(),
                    subjects: vec!["m".to_string()],
                    target: Some("b".to_string()),
                }],
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "w".to_string(), "b".to_string()],
                ..OracleClause::default()
            }],
            fold: Some(OracleFold {
                key_count: 1,
                aggs: vec![
                    agg(Agg::CountStar, None),
                    agg(Agg::Sum, Some(1)),
                    agg(Agg::Count, Some(2)),
                ],
            }),
        });
    }
    let mut after_folds: Vec<OracleRule> = Vec::new();
    if shape.prob {
        // A probability `e.w / 10.0`, in PROB_SCALE units.
        let tenths = |name: &str, var: &str| Computed {
            name: name.to_string(),
            vars: vec![var.to_string()],
            factor: PROB_SCALE / 10,
            constant: 0,
        };
        let edge_prob = || OracleClause {
            base: first.clone(),
            var_cols: var_cols(&[("a", 0), ("b", 1), ("w", 3)]),
            pos_refs: Vec::new(),
            neg_refs: Vec::new(),
            yield_vars: vec!["a".to_string(), "b".to_string(), "pb".to_string()],
            computed: vec![tenths("pb", "w")],
            prob_complements: Vec::new(),
            ..OracleClause::default()
        };
        let abp = || ["a", "b", "pb"].map(String::from).to_vec();
        stmts.push(clause(
            "pr",
            "({a}:Node)-[{e}:EDGE]->({b}:Node)",
            Vec::new(),
            "",
            vec![
                key_var("a"),
                key_var("b"),
                YieldItem {
                    prob: true,
                    ..item(false, "{e}.w / 10.0", "pb")
                },
            ],
        ));
        strata.push(vec![OracleRule {
            name: "pr".to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("b", ColKind::Node),
                ("pb", ColKind::Prob),
            ]),
            key_count: 2,
            clauses: vec![edge_prob()],
            fold: None,
            per_path: false,
            best: None,
        }]);
        for (name, function, aggregate) in [
            ("nor", "MNOR", Agg::NoisyOr),
            ("prd", "MPROD", Agg::Product),
        ] {
            stmts.push(clause(
                name,
                "({a}:Node)-[{e}:EDGE]->({b}:Node)",
                Vec::new(),
                &format!("FOLD {{{name}:pb}} = {function}({{e}}.w / 10.0)"),
                vec![key_var("a"), key_var("b"), own_col(name, "pb")],
            ));
            folds.push(OracleRule {
                name: name.to_string(),
                columns: columns(&[
                    ("a", ColKind::Node),
                    ("b", ColKind::Node),
                    ("pb", ColKind::Prob),
                ]),
                key_count: 2,
                clauses: vec![edge_prob()],
                fold: Some(OracleFold {
                    key_count: 2,
                    aggs: vec![agg(aggregate, Some(2))],
                }),
                per_path: false,
                best: None,
            });
        }
        // `hop`: one edge, then a fact of `nor`. The fold input does not read
        // `nor`'s probability, so the reference multiplies it in: a hop holds
        // only when the edge and the `nor` fact both do.
        stmts.push(clause(
            "hop",
            "({a}:Node)-[{e}:EDGE]->({m}:Node)",
            vec![is_ref(&["m"], "nor", Some("b"))],
            "FOLD {hop:hp} = MNOR({e}.w / 10.0)",
            vec![key_var("a"), key_var("b"), own_col("hop", "hp")],
        ));
        after_folds.push(OracleRule {
            name: "hop".to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("b", ColKind::Node),
                ("hp", ColKind::Prob),
            ]),
            key_count: 2,
            clauses: vec![OracleClause {
                base: first.clone(),
                var_cols: var_cols(&[("a", 0), ("m", 1), ("w", 3)]),
                pos_refs: vec![IsRef {
                    rule: "nor".to_string(),
                    subjects: vec!["m".to_string()],
                    target: Some("b".to_string()),
                    values: Vec::new(),
                }],
                neg_refs: Vec::new(),
                yield_vars: ["a", "b", "hp"].map(String::from).to_vec(),
                computed: vec![tenths("hp", "w")],
                prob_complements: Vec::new(),
                prob_factors: vec![ProbFactor {
                    reference: 0,
                    prob_column: 2,
                    prob_var: "hp".to_string(),
                }],
                ..OracleClause::default()
            }],
            fold: Some(OracleFold {
                key_count: 2,
                aggs: vec![agg(Agg::NoisyOr, Some(2))],
            }),
            per_path: false,
            best: None,
        });
        stmts.push(clause(
            "safe",
            "({a}:Node), ({b}:Node)",
            vec![is_not_ref(&["a"], "nor", Some("b"), false)],
            "",
            vec![
                key_var("a"),
                key_var("b"),
                YieldItem {
                    prob: true,
                    ..item(false, "1.0", "pb")
                },
            ],
        ));
        after_folds.push(OracleRule {
            name: "safe".to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("b", ColKind::Node),
                ("pb", ColKind::Prob),
            ]),
            key_count: 2,
            clauses: vec![OracleClause {
                base: all_pairs(shape.nodes),
                var_cols: var_cols(&[("a", 0), ("b", 1)]),
                pos_refs: Vec::new(),
                neg_refs: Vec::new(),
                yield_vars: abp(),
                computed: vec![Computed {
                    name: "pb".to_string(),
                    vars: Vec::new(),
                    factor: 1,
                    constant: PROB_SCALE,
                }],
                prob_complements: vec![ProbComplement {
                    reference: IsRef {
                        rule: "nor".to_string(),
                        subjects: vec!["a".to_string(), "b".to_string()],
                        target: None,
                        values: Vec::new(),
                    },
                    prob_column: 2,
                    prob_var: "pb".to_string(),
                }],
                ..OracleClause::default()
            }],
            fold: None,
            per_path: false,
            best: None,
        });
    }
    if !folds.is_empty() {
        strata.push(folds);
    }
    if !after_folds.is_empty() {
        strata.push(after_folds);
    }

    recursion_families(shape, &first, &mut stmts, &mut strata);
    let mut families = Families::new(shape, first, second);
    families.build(&mut stmts, &mut strata);

    // The graph: nodes with their properties, then each edge with its weight.
    let n = shape.nodes as i64;
    let mut parts: Vec<String> = (0..n)
        .map(|id| {
            format!(
                "(n{id}:Node {{id: {id}, k: {}, name: '{}'}})",
                (id + 1) % n,
                node_name(id)
            )
        })
        .collect();
    for (src, dst, w, second) in &shape.edges {
        let ty = if *second { "EDGE2" } else { "EDGE" };
        parts.push(format!("(n{src})-[:{ty} {{w: {w}}}]->(n{dst})"));
    }
    let strings = (0..n).map(|id| (node_name(id), id)).collect();

    finish(
        format!("CREATE {}", parts.join(", ")),
        SurfaceProgram { stmts },
        strata,
        strings,
        &families.family,
    )
}

// ---------------------------------------------------------------------------
// Binding families (issue #300)
// ---------------------------------------------------------------------------

/// A deterministic draw from a [`BindingShape`]'s entropy (splitmix64).
#[derive(Debug, Clone)]
struct Draw(u64);

impl Draw {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n`; `0` when `n == 0`.
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }

    /// True with probability `percent / 100`.
    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i + 1);
            items.swap(i, j);
        }
    }
}

/// Draws names for new variables and columns.
///
/// With capture-seeking on (family 3), half the draws reuse a name from the
/// candidates the caller offers — a referenced rule's column, a variable in
/// scope — so a body variable meets a column of the same name; otherwise every
/// name is fresh.
#[derive(Debug)]
struct NamePool {
    collide: bool,
    fresh: usize,
}

impl NamePool {
    fn fresh(&mut self) -> String {
        self.fresh += 1;
        format!("v{}", self.fresh)
    }

    /// A name not in `avoid`: one of `candidates` half the time when
    /// capture-seeking, else a fresh one.
    fn pick(&mut self, d: &mut Draw, candidates: &[&str], avoid: &[&str]) -> String {
        let options: Vec<&str> = candidates
            .iter()
            .copied()
            .filter(|c| !avoid.contains(c))
            .collect();
        if self.collide && !options.is_empty() && d.chance(50) {
            options[d.below(options.len())].to_string()
        } else {
            self.fresh()
        }
    }
}

/// Renames the role placeholders (upper-case names) of a template statement
/// to drawn names: variables, reference positions, `YIELD` columns and
/// output columns alike.
fn named(stmt: SurfaceStmt, roles: &[(&str, &str)]) -> SurfaceStmt {
    let map: HashMap<&str, &str> = roles.iter().copied().collect();
    let n = |s: &str| {
        map.get(s)
            .map_or_else(|| s.to_string(), |v| (*v).to_string())
    };
    let tmpl = |t: Tmpl| {
        Tmpl(
            t.0.into_iter()
                .map(|p| match p {
                    Piece::Var(v) => Piece::Var(n(&v)),
                    Piece::Col(r, c) => Piece::Col(r, n(&c)),
                    other => other,
                })
                .collect(),
        )
    };
    match stmt {
        SurfaceStmt::Clause(c) => SurfaceStmt::Clause(SurfaceClause {
            head: c.head,
            matches: tmpl(c.matches),
            conjuncts: c
                .conjuncts
                .into_iter()
                .map(|cj| match cj {
                    Conjunct::Filter(t) => Conjunct::Filter(tmpl(t)),
                    Conjunct::Ref(r) => Conjunct::Ref(SurfaceRef {
                        subjects: r.subjects.iter().map(|s| n(s)).collect(),
                        target: r.target.as_deref().map(n),
                        ..r
                    }),
                })
                .collect(),
            tail: tmpl(c.tail),
            yields: c
                .yields
                .into_iter()
                .map(|y| YieldItem {
                    expr: tmpl(y.expr),
                    column: n(&y.column),
                    ..y
                })
                .collect(),
        }),
        raw @ SurfaceStmt::Raw { .. } => raw,
    }
}

/// An `IsRef` from string slices.
fn isref(rule: &str, subjects: &[&str], target: Option<&str>) -> IsRef {
    IsRef {
        rule: rule.to_string(),
        subjects: subjects.iter().map(|s| (*s).to_string()).collect(),
        target: target.map(str::to_string),
        values: Vec::new(),
    }
}

/// Variable kinds from `(name, kind)` pairs.
fn var_kinds(pairs: &[(&str, ColKind)]) -> HashMap<String, ColKind> {
    pairs.iter().map(|(v, k)| ((*v).to_string(), *k)).collect()
}

/// A plain (set) rule.
fn plain_rule(
    name: &str,
    cols: &[(&str, ColKind)],
    key_count: usize,
    clauses: Vec<OracleClause>,
) -> OracleRule {
    OracleRule {
        name: name.to_string(),
        columns: columns(cols),
        key_count,
        clauses,
        fold: None,
        per_path: false,
        best: None,
    }
}

/// A clause whose `MATCH` is one node `var`, over the ids `ids`.
fn node_clause(var: &str, ids: impl Iterator<Item = i64>) -> OracleClause {
    OracleClause {
        base: ids.map(|id| vec![id]).collect(),
        var_cols: var_cols(&[(var, 0)]),
        var_kinds: var_kinds(&[(var, ColKind::Node)]),
        ..OracleClause::default()
    }
}

/// Builds the rules of the binding families a [`BindingShape`] enables.
struct Families {
    on: [bool; 7],
    d: Draw,
    names: NamePool,
    n: i64,
    first: Vec<Tuple>,
    second: Vec<Tuple>,
    /// Rules every family rule may reference, built from the graph alone.
    base: Vec<OracleRule>,
    /// The `ALONG` rule of family 5.
    recursive: Vec<OracleRule>,
    /// The readers: rules that reference the others.
    readers: Vec<OracleRule>,
    stmts: Vec<SurfaceStmt>,
    /// The value column of `seed`, once emitted.
    seed: Option<String>,
    /// Every rule a family emitted.
    family: BTreeSet<String>,
}

impl Families {
    fn new(shape: &RandomShape, first: Vec<Tuple>, second: Vec<Tuple>) -> Self {
        let on = shape.binding.families;
        Self {
            on,
            d: Draw(shape.binding.entropy),
            names: NamePool {
                collide: on[2],
                fresh: 0,
            },
            n: shape.nodes as i64,
            first,
            second,
            base: Vec::new(),
            recursive: Vec::new(),
            readers: Vec::new(),
            stmts: Vec::new(),
            seed: None,
            family: BTreeSet::new(),
        }
    }

    /// Emits every enabled family, appending text to `stmts` and strata to
    /// `strata`.
    fn build(&mut self, stmts: &mut Vec<SurfaceStmt>, strata: &mut Vec<Vec<OracleRule>>) {
        if self.on[0] {
            self.p1();
        }
        if self.on[1] {
            self.ref_bound();
        }
        if self.on[3] {
            self.shared_values();
        }
        if self.on[4] {
            self.prev_beside_value();
        }
        if self.on[5] {
            // Both kinds in most family cases. Each has a 15% coverage
            // floor; at 70/40 odds the string kind reached 29% of cases and
            // missed it in about one 96-case run in 1,100.
            let int = self.d.chance(75);
            let string = !int || self.d.chance(70);
            if int {
                self.scalar_keys(ColKind::Int);
            }
            if string {
                self.scalar_keys(ColKind::Str);
            }
        }
        if self.on[6] {
            self.negation_order();
        }
        stmts.append(&mut self.stmts);
        for stratum in [&mut self.base, &mut self.recursive, &mut self.readers] {
            if !stratum.is_empty() {
                strata.push(std::mem::take(stratum));
            }
        }
    }

    fn emit(&mut self, stmt: SurfaceStmt) {
        self.family.insert(stmt.rule().to_string());
        self.stmts.push(stmt);
    }

    /// A random cut in `0..n` over node ids: `id <= cut` and `id >= cut`
    /// both keep at least one node.
    fn cut(&mut self) -> i64 {
        self.d.below(self.n as usize) as i64
    }

    /// `seed(s, v)`: nodes with `id <= cut`, and `v = s.id` (an integer).
    /// Its names collide, when capture-seeking, with `reader_vars`. Returns
    /// the value column's name.
    fn ensure_seed(&mut self, reader_vars: &[&str]) -> String {
        if let Some(seed) = &self.seed {
            return seed.clone();
        }
        let key = self.names.pick(&mut self.d, reader_vars, &[]);
        let value = self.names.pick(&mut self.d, reader_vars, &[&key]);
        let cut = self.cut();
        self.emit(named(
            clause(
                "seed",
                "({S}:Node)",
                vec![filter(&format!("{{S}}.id <= {cut}"))],
                "",
                vec![key_var("S"), item(false, "{S}.id", "V")],
            ),
            &[("S", &key), ("V", &value)],
        ));
        let mut c = node_clause(&key, 0..=cut);
        c.computed = vec![Computed {
            name: value.clone(),
            vars: vec![key.clone()],
            factor: 1,
            constant: 0,
        }];
        c.yield_vars = vec![key.clone(), value.clone()];
        self.base.push(plain_rule(
            "seed",
            &[(&key, ColKind::Node), (&value, ColKind::Int)],
            1,
            vec![c],
        ));
        self.seed = Some(value.clone());
        value
    }

    /// Family 1 — P1 (#300): `o IS seed, (o, e, x) IS wt`, where `wt` is a
    /// FOLD keyed `(a, b)` with value `w`. The seed's KEY and the `wt`
    /// columns are named, when capture-seeking, like the reader's variables.
    fn p1(&mut self) {
        let (o, e, x) = (self.names.fresh(), self.names.fresh(), self.names.fresh());
        let reader = [o.as_str(), e.as_str(), x.as_str()];
        self.ensure_seed(&reader);
        let a = self.names.pick(&mut self.d, &reader, &[]);
        let b = self.names.pick(&mut self.d, &reader, &[&a]);
        let w = self.names.pick(&mut self.d, &reader, &[&a, &b]);
        let ed = self.names.fresh();
        self.emit(named(
            clause(
                "wt",
                "({A}:Node)-[{ED}:EDGE]->({B}:Node)",
                Vec::new(),
                "FOLD {wt:W} = MAX({ED}.w)",
                vec![key_var("A"), key_var("B"), own_col("wt", "W")],
            ),
            &[("A", &a), ("B", &b), ("W", &w), ("ED", &ed)],
        ));
        self.base.push(OracleRule {
            name: "wt".to_string(),
            columns: columns(&[(&a, ColKind::Node), (&b, ColKind::Node), (&w, ColKind::Int)]),
            key_count: 2,
            clauses: vec![OracleClause {
                base: self.first.clone(),
                var_cols: var_cols(&[("__a", 0), ("__b", 1), ("__w", 3)]),
                var_kinds: var_kinds(&[("__a", ColKind::Node), ("__b", ColKind::Node)]),
                yield_vars: ["__a", "__b", "__w"].map(String::from).to_vec(),
                ..OracleClause::default()
            }],
            fold: Some(OracleFold {
                key_count: 2,
                aggs: vec![FoldAgg {
                    agg: Agg::Max,
                    input: Some(2),
                }],
            }),
            per_path: false,
            best: None,
        });

        let mut conj = vec![
            is_ref(&["O"], "seed", None),
            is_ref(&["O", "E", "X"], "wt", None),
        ];
        self.d.shuffle(&mut conj);
        self.emit(named(
            clause(
                P1_READER,
                "({O}:Node)",
                conj,
                "",
                vec![key_var("E"), value_var("X")],
            ),
            &[("O", &o), ("E", &e), ("X", &x)],
        ));
        let mut c = node_clause(&o, 0..self.n);
        c.pos_refs = vec![isref("seed", &[&o], None), isref("wt", &[&o, &e, &x], None)];
        c.yield_vars = vec![e.clone(), x.clone()];
        self.readers.push(plain_rule(
            P1_READER,
            &[(&e, ColKind::Node), (&x, ColKind::Int)],
            1,
            vec![c],
        ));
    }

    /// Family 2 — subjects bound by a reference: a `TO` chain over `link`,
    /// a tuple naming `link`'s KEYs against the grain, and a KEY subject named
    /// like `seed`'s value column.
    fn ref_bound(&mut self) {
        // ch: x IS link TO mid, mid IS link TO z.
        let x = self.names.pick(&mut self.d, &["a", "b"], &[]);
        let z = self.names.pick(&mut self.d, &["a", "b"], &[&x]);
        // `mid` may merge with `x` or `z` (a repeated variable tests itself).
        let mid = self
            .names
            .pick(&mut self.d, &["a", "b", x.as_str(), z.as_str()], &[]);
        let mut conj = vec![
            is_ref(&["X"], "link", Some("MID")),
            is_ref(&["MID"], "link", Some("Z")),
        ];
        // Sometimes a third reference, exposing seed's value column.
        let seeded = self.d.chance(40);
        if seeded {
            self.ensure_seed(&[&x, &z, &mid]);
            conj.push(is_ref(&["X"], "seed", None));
        }
        self.d.shuffle(&mut conj);
        self.emit(named(
            clause(
                "ch",
                "({X}:Node)",
                conj,
                "",
                vec![key_var("X"), key_var("Z")],
            ),
            &[("X", &x), ("Z", &z), ("MID", &mid)],
        ));
        let mut c = node_clause(&x, 0..self.n);
        c.pos_refs = vec![
            isref("link", &[&x], Some(&mid)),
            isref("link", &[&mid], Some(&z)),
        ];
        if seeded {
            c.pos_refs.push(isref("seed", &[&x], None));
        }
        c.yield_vars = vec![x.clone(), z.clone()];
        self.readers.push(plain_rule(
            "ch",
            &[(&x, ColKind::Node), (&z, ColKind::Node)],
            2,
            vec![c],
        ));

        // pm: (b, a) IS link — positional, against the grain of link's names.
        if self.d.chance(60) {
            let anchor = self.d.below(3);
            let cut = self.cut();
            let n = self.names.fresh();
            let pair = || is_ref(&["b", "a"], "link", None);
            let (matches, mut conj, mut c) = match anchor {
                0 => (
                    "({N}:Node)",
                    vec![filter(&format!("{{N}}.id <= {cut}")), pair()],
                    node_clause(&n, 0..=cut),
                ),
                1 => ("({b}:Node)", vec![pair()], node_clause("b", 0..self.n)),
                _ => ("({a}:Node)", vec![pair()], node_clause("a", 0..self.n)),
            };
            self.d.shuffle(&mut conj);
            self.emit(named(
                clause("pm", matches, conj, "", vec![key_var("b"), key_var("a")]),
                &[("N", &n)],
            ));
            c.pos_refs = vec![isref("link", &["b", "a"], None)];
            c.yield_vars = vec!["b".to_string(), "a".to_string()];
            self.readers.push(plain_rule(
                "pm",
                &[("b", ColKind::Node), ("a", ColKind::Node)],
                2,
                vec![c],
            ));
        }

        // p4: o IS seed, (o, v) IS link, with `v` named like seed's value.
        if self.d.chance(60) {
            let v = self.ensure_seed(&[]);
            let o = self.names.pick(&mut self.d, &["a", "b"], &[&v]);
            let mut conj = vec![
                is_ref(&["O"], "seed", None),
                is_ref(&["O", "V"], "link", None),
            ];
            self.d.shuffle(&mut conj);
            self.emit(named(
                clause(
                    "p4",
                    "({O}:Node)",
                    conj,
                    "",
                    vec![key_var("O"), key_var("V")],
                ),
                &[("O", &o), ("V", &v)],
            ));
            let mut c = node_clause(&o, 0..self.n);
            c.pos_refs = vec![isref("seed", &[&o], None), isref("link", &[&o, &v], None)];
            c.yield_vars = vec![o.clone(), v.clone()];
            self.readers.push(plain_rule(
                "p4",
                &[(&o, ColKind::Node), (&v, ColKind::Node)],
                2,
                vec![c],
            ));
        }
    }

    /// Family 4 — `r1` and `r2` both yield a value column `sc`. The reader
    /// exposes it twice without reading it, binds both positionally, reads
    /// it where only `r1` exposes it, or reads it where both do (about one
    /// family case in seven): an [`OracleError::AmbiguousRuleColumn`].
    fn shared_values(&mut self) {
        let sc = self.names.pick(&mut self.d, &["a", "b"], &[]);
        let k1 = self.names.pick(&mut self.d, &["a", "b"], &[&sc]);
        let k2 = self.names.pick(&mut self.d, &["a", "b"], &[&sc]);
        let cut = self.cut();
        self.emit(named(
            clause(
                "r1",
                "({A}:Node)",
                vec![filter(&format!("{{A}}.id >= {cut}"))],
                "",
                vec![key_var("A"), item(false, "{A}.id", "SC")],
            ),
            &[("A", &k1), ("SC", &sc)],
        ));
        let mut c1 = node_clause(&k1, cut..self.n);
        c1.computed = vec![Computed {
            name: sc.clone(),
            vars: vec![k1.clone()],
            factor: 1,
            constant: 0,
        }];
        c1.yield_vars = vec![k1.clone(), sc.clone()];
        self.base.push(plain_rule(
            "r1",
            &[(&k1, ColKind::Node), (&sc, ColKind::Int)],
            1,
            vec![c1],
        ));
        self.emit(named(
            clause(
                "r2",
                "({A}:Node)",
                Vec::new(),
                "",
                vec![key_var("A"), item(false, "{A}.k", "SC")],
            ),
            &[("A", &k2), ("SC", &sc)],
        ));
        self.base.push(plain_rule(
            "r2",
            &[(&k2, ColKind::Node), (&sc, ColKind::Int)],
            1,
            vec![OracleClause {
                base: (0..self.n).map(|id| vec![id, (id + 1) % self.n]).collect(),
                var_cols: var_cols(&[("__a", 0), ("__k", 1)]),
                var_kinds: var_kinds(&[("__a", ColKind::Node), ("__k", ColKind::Int)]),
                yield_vars: vec!["__a".to_string(), "__k".to_string()],
                ..OracleClause::default()
            }],
        ));

        let q = self
            .names
            .pick(&mut self.d, &["a", "b", k1.as_str(), k2.as_str()], &[&sc]);
        let read = |op: CmpOp, constant: i64| Filter {
            lhs: Operand::Var(sc.clone()),
            op,
            rhs: Operand::Const(constant),
        };
        let mut c = node_clause(&q, 0..self.n);
        // Names the chosen variant adds, beside `Q` and `SC`.
        let (s1, s2) = (
            self.names.pick(&mut self.d, &[sc.as_str()], &[&q]),
            self.names.fresh(),
        );
        let t = self.names.pick(&mut self.d, &["a", "b"], &[&sc, &q]);
        let variant = self.d.below(100);
        let (mut conj, yields, cols): (Vec<Conjunct>, Vec<YieldItem>, Vec<(String, ColKind)>) =
            if variant < 15 {
                // Ambiguous: both references expose `sc`, and it is read.
                c.pos_refs = vec![isref("r1", &[&q], None), isref("r2", &[&q], None)];
                c.bare_reads = vec![ColumnRead::new(&sc, &sc)];
                c.filters = vec![read(CmpOp::Gt, 1)];
                c.yield_vars = vec![q.clone(), sc.clone()];
                (
                    vec![
                        is_ref(&["Q"], "r1", None),
                        is_ref(&["Q"], "r2", None),
                        filter("{r1:SC} > 1"),
                    ],
                    vec![key_var("Q"), item(false, "{r1:SC}", "SC")],
                    vec![(q.clone(), ColKind::Node), (sc.clone(), ColKind::Int)],
                )
            } else if variant < 55 {
                // Exposed twice, never read: legal.
                c.pos_refs = vec![isref("r1", &[&q], None), isref("r2", &[&q], None)];
                c.yield_vars = vec![q.clone()];
                (
                    vec![is_ref(&["Q"], "r1", None), is_ref(&["Q"], "r2", None)],
                    vec![key_var("Q")],
                    vec![(q.clone(), ColKind::Node)],
                )
            } else if variant < 75 {
                // Both bound positionally.
                c.pos_refs = vec![isref("r1", &[&q, &s1], None), isref("r2", &[&q, &s2], None)];
                c.filters = vec![Filter {
                    lhs: Operand::Var(s1.clone()),
                    op: CmpOp::Gt,
                    rhs: Operand::Var(s2.clone()),
                }];
                c.yield_vars = vec![q.clone(), s1.clone(), s2.clone()];
                (
                    vec![
                        is_ref(&["Q", "S1"], "r1", None),
                        is_ref(&["Q", "S2"], "r2", None),
                        filter("{S1} > {S2}"),
                    ],
                    vec![key_var("Q"), value_var("S1"), value_var("S2")],
                    vec![
                        (q.clone(), ColKind::Node),
                        (s1.clone(), ColKind::Int),
                        (s2.clone(), ColKind::Int),
                    ],
                )
            } else {
                // Read where only `r1` exposes it, beside a `link` whose KEY
                // columns may share its name (KEYs are never exposed).
                c.pos_refs = vec![isref("r1", &[&q], None), isref("link", &[&q], Some(&t))];
                c.bare_reads = vec![ColumnRead::new(&sc, &sc)];
                c.filters = vec![read(CmpOp::Ge, 1)];
                c.yield_vars = vec![q.clone(), t.clone(), sc.clone()];
                (
                    vec![
                        is_ref(&["Q"], "r1", None),
                        is_ref(&["Q"], "link", Some("T")),
                        filter("{r1:SC} >= 1"),
                    ],
                    vec![key_var("Q"), key_var("T"), item(false, "{r1:SC}", "SC")],
                    vec![
                        (q.clone(), ColKind::Node),
                        (t.clone(), ColKind::Node),
                        (sc.clone(), ColKind::Int),
                    ],
                )
            };
        self.d.shuffle(&mut conj);
        self.emit(named(
            clause("sh", "({Q}:Node)", conj, "", yields),
            &[("Q", &q), ("SC", &sc), ("S1", &s1), ("S2", &s2), ("T", &t)],
        ));
        let key_count = cols.iter().filter(|(_, k)| *k == ColKind::Node).count();
        let spec: Vec<(&str, ColKind)> = cols.iter().map(|(n, k)| (n.as_str(), *k)).collect();
        self.readers
            .push(plain_rule("sh", &spec, key_count, vec![c]));
    }

    /// Family 5 — Q2: an `ALONG` rule over edges of either type whose
    /// recursive clause reads `prev.q` beside `a IS dsc`, and `dsc` also
    /// yields a column `q`.
    fn prev_beside_value(&mut self) {
        let q = self.names.fresh();
        let dk = self.names.pick(&mut self.d, &["a", "b", "m"], &[]);
        // Most nodes: the clause must reach recursive paths to show `prev`.
        let cut = self.n / 2 + self.d.below((self.n - self.n / 2) as usize) as i64;
        self.emit(named(
            clause(
                "dsc",
                "({K}:Node)",
                vec![filter(&format!("{{K}}.id <= {cut}"))],
                "",
                vec![key_var("K"), item(false, "{K}.k", "Q")],
            ),
            &[("K", &dk), ("Q", &q)],
        ));
        self.base.push(plain_rule(
            "dsc",
            &[(&dk, ColKind::Node), (&q, ColKind::Int)],
            1,
            vec![OracleClause {
                base: (0..=cut).map(|id| vec![id, (id + 1) % self.n]).collect(),
                var_cols: var_cols(&[("__a", 0), ("__k", 1)]),
                var_kinds: var_kinds(&[("__a", ColKind::Node), ("__k", ColKind::Int)]),
                yield_vars: vec!["__a".to_string(), "__k".to_string()],
                ..OracleClause::default()
            }],
        ));
        self.emit(named(
            clause(
                PREV_RULE,
                "({a}:Node)-[{e}]->({b}:Node)",
                vec![filter("{a}.id < {b}.id")],
                "ALONG {pth:Q} = {e}.w",
                vec![key_var("a"), key_var("b"), own_col(PREV_RULE, "Q")],
            ),
            &[("Q", &q)],
        ));
        let mut conj = vec![
            filter("{a}.id < {m}.id"),
            is_ref(&["a"], "dsc", None),
            is_ref(&["m"], PREV_RULE, Some("b")),
        ];
        self.d.shuffle(&mut conj);
        self.emit(named(
            clause(
                PREV_RULE,
                "({a}:Node)-[{e}]->({m}:Node)",
                conj,
                "ALONG {pth:Q} = prev.{pth:Q} + {e}.w",
                vec![key_var("a"), key_var("b"), own_col(PREV_RULE, "Q")],
            ),
            &[("Q", &q)],
        ));
        let step = |vars: &[&str]| Computed {
            name: "__v".to_string(),
            vars: vars.iter().map(|v| (*v).to_string()).collect(),
            factor: 1,
            constant: 0,
        };
        let abv = || ["a", "b", "__v"].map(String::from).to_vec();
        // Edges of either type to a greater id: per-path values need no
        // cycles, and both types make the recursive paths `prev` needs
        // denser.
        let acyclic: Vec<Tuple> = self
            .first
            .iter()
            .chain(&self.second)
            .filter(|e| e[0] < e[1])
            .cloned()
            .collect();
        self.recursive.push(OracleRule {
            name: PREV_RULE.to_string(),
            columns: columns(&[
                ("a", ColKind::Node),
                ("b", ColKind::Node),
                (&q, ColKind::Int),
            ]),
            key_count: 2,
            clauses: vec![
                OracleClause {
                    base: acyclic.clone(),
                    var_cols: var_cols(&[("a", 0), ("b", 1), ("__w", 3)]),
                    var_kinds: var_kinds(&[("a", ColKind::Node), ("b", ColKind::Node)]),
                    computed: vec![step(&["__w"])],
                    yield_vars: abv(),
                    ..OracleClause::default()
                },
                OracleClause {
                    base: acyclic,
                    var_cols: var_cols(&[("a", 0), ("m", 1), ("__w", 3)]),
                    var_kinds: var_kinds(&[("a", ColKind::Node), ("m", ColKind::Node)]),
                    pos_refs: vec![
                        isref("dsc", &["a"], None),
                        isref(PREV_RULE, &["m"], Some("b")),
                    ],
                    prev_reads: vec![ColumnRead::new(&q, "__p")],
                    computed: vec![step(&["__p", "__w"])],
                    yield_vars: abv(),
                    ..OracleClause::default()
                },
            ],
            fold: None,
            per_path: true,
            best: None,
        });
    }

    /// Family 6 — scalar KEYs: `tg(n, c)` keyed by an integer (`t.k`, every
    /// value the VID of another node) or a string (`t.name`), a ban list `bn`
    /// of the same kind, and a reader binding the scalar through `TO` or a
    /// tuple, with or without the negation.
    fn scalar_keys(&mut self, kind: ColKind) {
        let (tg, bn, sk, prop) = if kind == ColKind::Int {
            ("tgi", "bni", SCALAR_INT_READER, "k")
        } else {
            ("tgs", "bns", SCALAR_STR_READER, "name")
        };
        let a = self.names.fresh();
        let col = self.names.fresh();
        let value = |id: i64, n: i64| {
            if kind == ColKind::Int {
                (id + 1) % n
            } else {
                id
            }
        };
        let n = self.n;
        for (rule, edges, ty) in [
            (tg, self.first.clone(), "EDGE"),
            (bn, self.second.clone(), "EDGE2"),
        ] {
            let rn = self.names.pick(&mut self.d, &[&a, &col], &[]);
            let rc = self.names.pick(&mut self.d, &[&col, &a], &[&rn]);
            let t = self.names.fresh();
            let mut yields = vec![key_var("N"), item(true, &format!("{{T}}.{prop}"), "C")];
            let mut cols = vec![(rn.clone(), ColKind::Node), (rc.clone(), kind)];
            let mut row = vec!["__n".to_string(), "__c".to_string()];
            // `tg` also yields a value column, exposed to the reader and
            // named, when capture-seeking, like one of its variables (P2).
            let tv = self.names.pick(&mut self.d, &[&a, &col], &[&rn, &rc]);
            if rule == tg {
                yields.push(item(false, "{T}.id", "TV"));
                cols.push((tv.clone(), ColKind::Int));
                row.push("__t".to_string());
            }
            self.emit(named(
                clause(
                    rule,
                    &format!("({{N}}:Node)-[:{ty}]->({{T}}:Node)"),
                    Vec::new(),
                    "",
                    yields,
                ),
                &[("N", &rn), ("C", &rc), ("T", &t), ("TV", &tv)],
            ));
            let spec: Vec<(&str, ColKind)> = cols.iter().map(|(c, k)| (c.as_str(), *k)).collect();
            self.base.push(plain_rule(
                rule,
                &spec,
                2,
                vec![OracleClause {
                    base: edges
                        .iter()
                        .map(|e| vec![e[0], value(e[1], n), e[1]])
                        .collect(),
                    var_cols: var_cols(&[("__n", 0), ("__c", 1), ("__t", 2)]),
                    var_kinds: var_kinds(&[("__n", ColKind::Node), ("__c", kind)]),
                    yield_vars: row,
                    ..OracleClause::default()
                }],
            ));
        }
        let negate = self.d.chance(35);
        let tuple = self.d.chance(40);
        let prefix = self.d.chance(50);
        let mut c = node_clause(&a, 0..self.n);
        let mut conj = Vec::new();
        if tuple {
            conj.push(is_ref(&["A", "COL"], tg, None));
            c.pos_refs = vec![isref(tg, &[&a, &col], None)];
            if negate {
                conj.push(is_not_ref(&["A", "COL"], bn, None, prefix));
                c.neg_refs = vec![isref(bn, &[&a, &col], None)];
            }
        } else {
            conj.push(is_ref(&["A"], tg, Some("COL")));
            c.pos_refs = vec![isref(tg, &[&a], Some(&col))];
            if negate {
                conj.push(is_not_ref(&["A"], bn, Some("COL"), prefix));
                c.neg_refs = vec![isref(bn, &[&a], Some(&col))];
            }
        }
        self.d.shuffle(&mut conj);
        c.yield_vars = vec![a.clone(), col.clone()];
        self.emit(named(
            clause(
                sk,
                "({A}:Node)",
                conj,
                "",
                vec![key_var("A"), key_var("COL")],
            ),
            &[("A", &a), ("COL", &col)],
        ));
        self.readers.push(plain_rule(
            sk,
            &[(&a, ColKind::Node), (&col, kind)],
            2,
            vec![c],
        ));
    }

    /// Family 7 — `IS NOT` before or after the reference that binds its
    /// subject, as `NOT y IS blk` or `y IS NOT blk`.
    fn negation_order(&mut self) {
        let a = self.names.pick(&mut self.d, &["a", "b"], &[]);
        let y = self.names.pick(&mut self.d, &["a", "b"], &[&a]);
        let bk = self.names.pick(&mut self.d, &[y.as_str(), a.as_str()], &[]);
        let cut = self.cut();
        self.emit(named(
            clause(
                "blk",
                "({B}:Node)",
                vec![filter(&format!("{{B}}.id >= {cut}"))],
                "",
                vec![key_var("B")],
            ),
            &[("B", &bk)],
        ));
        let mut blk = node_clause(&bk, cut..self.n);
        blk.yield_vars = vec![bk.clone()];
        self.base
            .push(plain_rule("blk", &[(&bk, ColKind::Node)], 1, vec![blk]));
        let prefix = self.d.chance(50);
        let mut conj = vec![
            is_not_ref(&["Y"], "blk", None, prefix),
            is_ref(&["A"], "link", Some("Y")),
        ];
        let seeded = self.d.chance(40);
        if seeded {
            self.ensure_seed(&[&a, &y]);
            conj.push(is_ref(&["A"], "seed", None));
        }
        self.d.shuffle(&mut conj);
        self.emit(named(
            clause(
                "ng",
                "({A}:Node)",
                conj,
                "",
                vec![key_var("A"), key_var("Y")],
            ),
            &[("A", &a), ("Y", &y)],
        ));
        let mut c = node_clause(&a, 0..self.n);
        c.pos_refs = vec![isref("link", &[&a], Some(&y))];
        if seeded {
            c.pos_refs.push(isref("seed", &[&a], None));
        }
        c.neg_refs = vec![isref("blk", &[&y], None)];
        c.yield_vars = vec![a.clone(), y.clone()];
        self.readers.push(plain_rule(
            "ng",
            &[(&a, ColKind::Node), (&y, ColKind::Node)],
            2,
            vec![c],
        ));
    }
}

/// The variables a template mentions.
fn tmpl_vars(t: &Tmpl) -> BTreeSet<&str> {
    t.0.iter()
        .filter_map(|p| match p {
            Piece::Var(v) => Some(v.as_str()),
            _ => None,
        })
        .collect()
}

/// The `(column index, variable)` positions a reference names: subjects,
/// then the target.
fn ref_positions(r: &SurfaceRef) -> Vec<(usize, &str)> {
    r.subjects
        .iter()
        .chain(&r.target)
        .map(String::as_str)
        .enumerate()
        .collect()
}

/// The value columns a positive reference exposes by bare name: value
/// columns at positions it does not name. KEY columns are never exposed.
#[must_use]
pub fn exposed_columns<'a>(r: &SurfaceRef, rule: &'a OracleRule) -> Vec<&'a str> {
    let named = r.subjects.len() + usize::from(r.target.is_some());
    rule.columns
        .iter()
        .skip(named.max(rule.key_count))
        .map(|c| c.name.as_str())
        .collect()
}

/// The shapes each binding-family rule exercises, read off its rendered
/// (canonical) clauses and the referenced rules' schemas.
fn derive_features(
    surface: &SurfaceProgram,
    program: &OracleProgram,
    family: &BTreeSet<String>,
) -> HashMap<String, BTreeSet<Feature>> {
    let rules: HashMap<&str, &OracleRule> = program
        .strata
        .iter()
        .flatten()
        .map(|r| (r.name.as_str(), r))
        .collect();
    let mut out: HashMap<String, BTreeSet<Feature>> = HashMap::new();
    for (_, c) in surface.clauses() {
        if !family.contains(&c.head) {
            continue;
        }
        let mut f = BTreeSet::new();
        let matched = tmpl_vars(&c.matches);
        let refs: Vec<(usize, &SurfaceRef, &OracleRule)> = c
            .refs()
            .filter_map(|(i, r)| rules.get(r.rule.as_str()).map(|rule| (i, r, *rule)))
            .collect();
        if refs.iter().filter(|(_, r, _)| !r.negated).count() >= 2 {
            f.insert(Feature::MultiRef);
        }
        let mut exposed: HashMap<&str, usize> = HashMap::new();
        for &(i, r, rule) in &refs {
            let positions = ref_positions(r);
            for &(p, v) in &positions {
                if p < rule.key_count {
                    match rule.columns[p].kind {
                        ColKind::Int => {
                            f.insert(Feature::ScalarKeyInt);
                        }
                        ColKind::Str => {
                            f.insert(Feature::ScalarKeyStr);
                        }
                        ColKind::Node | ColKind::Prob => {}
                    }
                }
                if r.negated || matched.contains(v) {
                    continue;
                }
                let is_target = r.target.is_some() && p == r.subjects.len();
                if !is_target {
                    f.insert(if p < rule.key_count {
                        Feature::RefBoundKey
                    } else {
                        Feature::RefBoundValue
                    });
                } else if refs
                    .iter()
                    .any(|&(j, o, _)| j != i && ref_positions(o).iter().any(|&(_, w)| w == v))
                {
                    f.insert(Feature::RefBoundTo);
                }
            }
            if !r.negated
                && r.subjects.len() >= 2
                && r.subjects.iter().any(|s| !matched.contains(s.as_str()))
            {
                f.insert(Feature::RefBoundTuple);
            }
            if r.negated {
                f.insert(if r.not_prefix_form {
                    Feature::NegPrefixForm
                } else {
                    Feature::NegSuffixForm
                });
                for &(_, v) in &positions {
                    if matched.contains(v) {
                        continue;
                    }
                    let binder = refs
                        .iter()
                        .filter(|(_, o, _)| !o.negated)
                        .find(|(_, o, _)| ref_positions(o).iter().any(|&(_, w)| w == v))
                        .map(|&(j, _, _)| j);
                    match binder {
                        Some(j) if j > i => {
                            f.insert(Feature::NegBeforeBinder);
                        }
                        Some(_) => {
                            f.insert(Feature::NegAfterBinder);
                        }
                        None => {}
                    }
                }
            } else {
                for col in exposed_columns(r, rule) {
                    *exposed.entry(col).or_default() += 1;
                }
            }
        }
        if exposed.values().any(|&n| n >= 2) {
            f.insert(Feature::SharedValueName);
        }
        for v in c.vars() {
            let collides = refs.iter().any(|&(_, r, rule)| {
                let positions = ref_positions(r);
                rule.columns.iter().enumerate().any(|(p, col)| {
                    col.name == v
                        && positions.iter().find(|&&(q, _)| q == p).map(|&(_, w)| w)
                            != Some(v.as_str())
                })
            });
            if collides {
                f.insert(Feature::NameCollision);
            }
        }
        // `prev.q` beside another reference exposing `q`.
        for w in c.tail.0.windows(2) {
            if let [Piece::Lit(lit), Piece::Col(rule, q)] = w
                && lit.ends_with("prev.")
                && rule == &c.head
                && refs.iter().any(|&(_, r, other)| {
                    !r.negated
                        && r.rule != c.head
                        && exposed_columns(r, other).contains(&q.as_str())
                })
            {
                f.insert(Feature::PrevBesideValue);
            }
        }
        out.entry(c.head.clone()).or_default().extend(f);
    }
    if let Err(OracleError::AmbiguousRuleColumn { rule, .. }) = check_program(program) {
        out.entry(rule)
            .or_default()
            .insert(Feature::AmbiguousExpected);
    }
    out
}

// ---------------------------------------------------------------------------
// Recursion families (P5: non-linear and mutual recursion)
// ---------------------------------------------------------------------------

/// Which recursion families a random program adds, aimed at how a recursive
/// stratum feeds its references: clauses with two references into their own
/// stratum (non-linear), and strata of two rules that read each other
/// (mutual), each combined with `BEST BY`, `FOLD`, `ALONG` and `IS NOT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecursionShape {
    /// `nlr`: non-linear transitive closure, `a IS nlr TO m, m IS nlr TO b`.
    pub nonlinear: bool,
    /// `nlb`: non-linear path weight under `BEST BY`. `Some(true)` keeps the
    /// least over every edge (cycles included), `Some(false)` the greatest
    /// over the acyclic edges.
    pub nonlinear_best: Option<bool>,
    /// `mua` / `mub`: odd and even walks, mutually recursive, each with a
    /// non-linear clause (`mub` reads `mua` twice, `mua` reads both).
    pub mutual: bool,
    /// `mba` / `mbb`: the least odd and even walk weights, mutually recursive
    /// under `BEST BY`, `mbb` with a non-linear clause.
    pub mutual_best: bool,
    /// `mfa` / `mfb`: mutually recursive `MSUM`s over the acyclic edges, each
    /// reading the other's folded value; `mfa` has a non-linear clause.
    pub mutual_fold: bool,
    /// `mpa` / `mpb`: a per-path `ALONG` rule and a plain rule reading each
    /// other over the acyclic edges.
    pub mutual_along: bool,
    /// Adds `m IS NOT xcl` (nodes with `id >= cut`) to recursive clauses.
    pub negation: bool,
    /// Seeds the cut of `xcl`.
    pub entropy: u64,
}

impl RecursionShape {
    /// No recursion family.
    #[must_use]
    pub fn none() -> Self {
        Self {
            nonlinear: false,
            nonlinear_best: None,
            mutual: false,
            mutual_best: false,
            mutual_fold: false,
            mutual_along: false,
            negation: false,
            entropy: 0,
        }
    }
}

/// `name = sum of vars` (factor 1, no constant).
fn sum_of(name: &str, vars: &[&str]) -> Computed {
    Computed {
        name: name.to_string(),
        vars: vars.iter().map(|v| (*v).to_string()).collect(),
        factor: 1,
        constant: 0,
    }
}

/// An `IsRef` whose subjects reach past the KEYs (a tuple subject).
fn tuple_ref(rule: &str, subjects: &[&str]) -> IsRef {
    isref(rule, subjects, None)
}

/// Builds the rules of the recursion families `shape.recursion` enables.
///
/// `first` holds the first relationship type's edges as
/// `[src, dst, edge index, weight]`. Every family gets its own stratum (a
/// mutual pair shares one), after an optional `xcl` stratum the negations
/// read.
fn recursion_families(
    shape: &RandomShape,
    first: &[Tuple],
    stmts: &mut Vec<SurfaceStmt>,
    strata: &mut Vec<Vec<OracleRule>>,
) {
    let rec = shape.recursion;
    let n = shape.nodes as i64;
    let dag: Vec<Tuple> = first.iter().filter(|e| e[0] < e[1]).cloned().collect();
    let all_nodes: Vec<Tuple> = (0..n).map(|id| vec![id]).collect();
    let edge_cols = |a: &str, b: &str| var_cols(&[(a, 0), (b, 1), ("w", 3)]);
    let node_kind = |v: &str| var_kinds(&[(v, ColKind::Node)]);

    // `xcl(x)`: nodes with `id >= cut`, cut in 1..=n so node 0 is never
    // excluded.
    let any_family = rec.nonlinear
        || rec.nonlinear_best.is_some()
        || rec.mutual
        || rec.mutual_best
        || rec.mutual_fold
        || rec.mutual_along;
    let negation = rec.negation && any_family;
    if negation {
        let cut = 1 + (rec.entropy % n.max(1) as u64) as i64;
        stmts.push(clause(
            "xcl",
            "({x}:Node)",
            vec![filter(&format!("{{x}}.id >= {cut}"))],
            "",
            vec![key_var("x")],
        ));
        strata.push(vec![plain_rule(
            "xcl",
            &[("x", ColKind::Node)],
            1,
            vec![OracleClause {
                yield_vars: vec!["x".to_string()],
                ..node_clause("x", cut..n)
            }],
        )]);
    }
    // `var IS NOT xcl`, in surface and IR form, when negation is on.
    let neg_surface = |var: &str| -> Vec<Conjunct> {
        if negation {
            vec![is_not_ref(&[var], "xcl", None, false)]
        } else {
            Vec::new()
        }
    };
    let neg_ir = |var: &str| -> Vec<IsRef> {
        if negation {
            vec![isref("xcl", &[var], None)]
        } else {
            Vec::new()
        }
    };
    let ab = || vec!["a".to_string(), "b".to_string()];
    let abd = |d: &str| vec!["a".to_string(), "b".to_string(), d.to_string()];

    if rec.nonlinear {
        stmts.push(clause(
            "nlr",
            "({a}:Node)-[:EDGE]->({b}:Node)",
            Vec::new(),
            "",
            vec![key_var("a"), key_var("b")],
        ));
        let mut conj = vec![
            is_ref(&["a"], "nlr", Some("m")),
            is_ref(&["m"], "nlr", Some("b")),
        ];
        conj.extend(neg_surface("m"));
        stmts.push(clause(
            "nlr",
            "({a}:Node)",
            conj,
            "",
            vec![key_var("a"), key_var("b")],
        ));
        strata.push(vec![plain_rule(
            "nlr",
            &[("a", ColKind::Node), ("b", ColKind::Node)],
            2,
            vec![
                OracleClause {
                    base: first.to_vec(),
                    var_cols: var_cols(&[("a", 0), ("b", 1)]),
                    yield_vars: ab(),
                    ..OracleClause::default()
                },
                OracleClause {
                    base: all_nodes.clone(),
                    var_cols: var_cols(&[("a", 0)]),
                    var_kinds: node_kind("a"),
                    pos_refs: vec![
                        isref("nlr", &["a"], Some("m")),
                        isref("nlr", &["m"], Some("b")),
                    ],
                    neg_refs: neg_ir("m"),
                    yield_vars: ab(),
                    ..OracleClause::default()
                },
            ],
        )]);
    }

    if let Some(ascending) = rec.nonlinear_best {
        let (order, base) = if ascending {
            ("ASC", first.to_vec())
        } else {
            ("DESC", dag.clone())
        };
        let tail = format!("BEST BY {{nlb:d}} {order}");
        let base_filter = if ascending {
            Vec::new()
        } else {
            vec![filter("{a}.id < {b}.id")]
        };
        stmts.push(clause(
            "nlb",
            "({a}:Node)-[{e}:EDGE]->({b}:Node)",
            base_filter,
            &tail,
            vec![key_var("a"), key_var("b"), item(false, "{e}.w", "d")],
        ));
        let mut conj = vec![
            is_ref(&["a", "m", "p"], "nlb", None),
            is_ref(&["m", "b", "q"], "nlb", None),
        ];
        conj.extend(neg_surface("m"));
        stmts.push(clause(
            "nlb",
            "({a}:Node)",
            conj,
            &tail,
            vec![key_var("a"), key_var("b"), item(false, "{p} + {q}", "d")],
        ));
        strata.push(vec![OracleRule {
            best: Some(OracleBest {
                key_count: 2,
                criterion: 2,
                ascending,
            }),
            ..plain_rule(
                "nlb",
                &[
                    ("a", ColKind::Node),
                    ("b", ColKind::Node),
                    ("d", ColKind::Int),
                ],
                2,
                vec![
                    OracleClause {
                        base,
                        var_cols: edge_cols("a", "b"),
                        computed: vec![sum_of("d", &["w"])],
                        yield_vars: abd("d"),
                        ..OracleClause::default()
                    },
                    OracleClause {
                        base: all_nodes.clone(),
                        var_cols: var_cols(&[("a", 0)]),
                        var_kinds: node_kind("a"),
                        pos_refs: vec![
                            tuple_ref("nlb", &["a", "m", "p"]),
                            tuple_ref("nlb", &["m", "b", "q"]),
                        ],
                        neg_refs: neg_ir("m"),
                        computed: vec![sum_of("d", &["p", "q"])],
                        yield_vars: abd("d"),
                        ..OracleClause::default()
                    },
                ],
            )
        }]);
    }

    if rec.mutual {
        // Text order per rule matches the IR's clause order.
        stmts.push(clause(
            "mua",
            "({a}:Node)-[:EDGE]->({b}:Node)",
            Vec::new(),
            "",
            vec![key_var("a"), key_var("b")],
        ));
        let mut conj = vec![is_ref(&["m"], "mua", Some("b"))];
        conj.extend(neg_surface("m"));
        stmts.push(clause(
            "mub",
            "({a}:Node)-[:EDGE]->({m}:Node)",
            conj,
            "",
            vec![key_var("a"), key_var("b")],
        ));
        let mut conj = vec![is_ref(&["m"], "mub", Some("b"))];
        conj.extend(neg_surface("m"));
        stmts.push(clause(
            "mua",
            "({a}:Node)-[:EDGE]->({m}:Node)",
            conj,
            "",
            vec![key_var("a"), key_var("b")],
        ));
        let mut conj = vec![
            is_ref(&["a"], "mua", Some("m")),
            is_ref(&["m"], "mua", Some("b")),
        ];
        conj.extend(neg_surface("m"));
        stmts.push(clause(
            "mub",
            "({a}:Node)",
            conj,
            "",
            vec![key_var("a"), key_var("b")],
        ));
        stmts.push(clause(
            "mua",
            "({a}:Node)",
            vec![
                is_ref(&["a"], "mua", Some("m")),
                is_ref(&["m"], "mub", Some("b")),
            ],
            "",
            vec![key_var("a"), key_var("b")],
        ));
        let edge_hop = |other: &str| OracleClause {
            base: first.to_vec(),
            var_cols: edge_cols("a", "m"),
            pos_refs: vec![isref(other, &["m"], Some("b"))],
            neg_refs: neg_ir("m"),
            yield_vars: ab(),
            ..OracleClause::default()
        };
        let compose = |left: &str, right: &str, negated: bool| OracleClause {
            base: all_nodes.clone(),
            var_cols: var_cols(&[("a", 0)]),
            var_kinds: node_kind("a"),
            pos_refs: vec![
                isref(left, &["a"], Some("m")),
                isref(right, &["m"], Some("b")),
            ],
            neg_refs: if negated { neg_ir("m") } else { Vec::new() },
            yield_vars: ab(),
            ..OracleClause::default()
        };
        let cols = [("a", ColKind::Node), ("b", ColKind::Node)];
        strata.push(vec![
            plain_rule(
                "mua",
                &cols,
                2,
                vec![
                    OracleClause {
                        base: first.to_vec(),
                        var_cols: var_cols(&[("a", 0), ("b", 1)]),
                        yield_vars: ab(),
                        ..OracleClause::default()
                    },
                    edge_hop("mub"),
                    compose("mua", "mub", false),
                ],
            ),
            plain_rule(
                "mub",
                &cols,
                2,
                vec![edge_hop("mua"), compose("mua", "mua", true)],
            ),
        ]);
    }

    if rec.mutual_best {
        // `ALONG` carries the path weight here; `nlb` and `mpb` compute theirs
        // in YIELD, so both forms are covered.
        let tail = |rule: &str, value: &str| {
            format!("ALONG {{{rule}:d}} = {value} BEST BY {{{rule}:d}} ASC")
        };
        let yields = |rule: &str| vec![key_var("a"), key_var("b"), own_col(rule, "d")];
        stmts.push(clause(
            "mba",
            "({a}:Node)-[{e}:EDGE]->({b}:Node)",
            Vec::new(),
            &tail("mba", "{e}.w"),
            yields("mba"),
        ));
        let mut conj = vec![is_ref(&["m", "b", "p"], "mba", None)];
        conj.extend(neg_surface("m"));
        stmts.push(clause(
            "mbb",
            "({a}:Node)-[{e}:EDGE]->({m}:Node)",
            conj,
            &tail("mbb", "{p} + {e}.w"),
            yields("mbb"),
        ));
        stmts.push(clause(
            "mba",
            "({a}:Node)-[{e}:EDGE]->({m}:Node)",
            vec![is_ref(&["m", "b", "p"], "mbb", None)],
            &tail("mba", "{p} + {e}.w"),
            yields("mba"),
        ));
        stmts.push(clause(
            "mbb",
            "({a}:Node)",
            vec![
                is_ref(&["a", "m", "p"], "mba", None),
                is_ref(&["m", "b", "q"], "mba", None),
            ],
            &tail("mbb", "{p} + {q}"),
            yields("mbb"),
        ));
        let hop = |other: &str, negated: bool| OracleClause {
            base: first.to_vec(),
            var_cols: edge_cols("a", "m"),
            pos_refs: vec![tuple_ref(other, &["m", "b", "p"])],
            neg_refs: if negated { neg_ir("m") } else { Vec::new() },
            computed: vec![sum_of("d", &["p", "w"])],
            yield_vars: abd("d"),
            ..OracleClause::default()
        };
        let cols = [
            ("a", ColKind::Node),
            ("b", ColKind::Node),
            ("d", ColKind::Int),
        ];
        let best = Some(OracleBest {
            key_count: 2,
            criterion: 2,
            ascending: true,
        });
        strata.push(vec![
            OracleRule {
                best,
                ..plain_rule(
                    "mba",
                    &cols,
                    2,
                    vec![
                        OracleClause {
                            base: first.to_vec(),
                            var_cols: edge_cols("a", "b"),
                            computed: vec![sum_of("d", &["w"])],
                            yield_vars: abd("d"),
                            ..OracleClause::default()
                        },
                        hop("mbb", false),
                    ],
                )
            },
            OracleRule {
                best,
                ..plain_rule(
                    "mbb",
                    &cols,
                    2,
                    vec![
                        hop("mba", true),
                        OracleClause {
                            base: all_nodes.clone(),
                            var_cols: var_cols(&[("a", 0)]),
                            var_kinds: node_kind("a"),
                            pos_refs: vec![
                                tuple_ref("mba", &["a", "m", "p"]),
                                tuple_ref("mba", &["m", "b", "q"]),
                            ],
                            computed: vec![sum_of("d", &["p", "q"])],
                            yield_vars: abd("d"),
                            ..OracleClause::default()
                        },
                    ],
                )
            },
        ]);
    }

    if rec.mutual_fold {
        stmts.push(clause(
            "mfa",
            "({a}:Node)",
            Vec::new(),
            "",
            vec![key_var("a"), item(false, "1", "s")],
        ));
        let mut conj = vec![filter("{a}.id < {c}.id"), is_ref(&["c"], "mfa", None)];
        conj.extend(neg_surface("c"));
        stmts.push(clause(
            "mfb",
            "({a}:Node)-[{e}:EDGE]->({c}:Node)",
            conj,
            "FOLD {mfb:s} = MSUM({mfa:s} + {e}.w)",
            vec![key_var("a"), own_col("mfb", "s")],
        ));
        stmts.push(clause(
            "mfa",
            "({a}:Node)-[{e}:EDGE]->({c}:Node)",
            vec![filter("{a}.id < {c}.id"), is_ref(&["c"], "mfb", None)],
            "FOLD {mfa:s} = MSUM({mfb:s} + {e}.w)",
            vec![key_var("a"), own_col("mfa", "s")],
        ));
        stmts.push(clause(
            "mfa",
            "({c}:Node)<-[{e}:EDGE]-({a}:Node)-[{f}:EDGE]->({d}:Node)",
            vec![
                filter("{a}.id < {c}.id"),
                filter("{a}.id < {d}.id"),
                is_ref(&["c", "t"], "mfb", None),
                is_ref(&["d", "u"], "mfa", None),
            ],
            "FOLD {mfa:s} = MSUM({t} + {u} + {e}.w)",
            vec![key_var("a"), own_col("mfa", "s")],
        ));
        // Two distinct acyclic edges out of one node: `[a, c, w_e, d]`.
        // Relationship uniqueness within one MATCH keeps `e` and `f` apart.
        let fork: Vec<Tuple> = dag
            .iter()
            .flat_map(|e| {
                dag.iter()
                    .filter(move |f| f[0] == e[0] && f[2] != e[2])
                    .map(move |f| vec![e[0], e[1], e[3], f[1]])
            })
            .collect();
        let read_other = |other: &str, negated: bool| OracleClause {
            base: dag.clone(),
            var_cols: var_cols(&[("a", 0), ("c", 1), ("w", 3)]),
            pos_refs: vec![IsRef {
                values: vec!["cs".to_string()],
                ..isref(other, &["c"], None)
            }],
            neg_refs: if negated { neg_ir("c") } else { Vec::new() },
            computed: vec![sum_of("x", &["cs", "w"])],
            yield_vars: vec!["a".to_string(), "x".to_string()],
            ..OracleClause::default()
        };
        let fold = Some(OracleFold {
            key_count: 1,
            aggs: vec![FoldAgg {
                agg: Agg::Sum,
                input: Some(1),
            }],
        });
        let cols = [("a", ColKind::Node), ("s", ColKind::Int)];
        strata.push(vec![
            OracleRule {
                fold: fold.clone(),
                ..plain_rule(
                    "mfa",
                    &cols,
                    1,
                    vec![
                        OracleClause {
                            base: all_nodes.clone(),
                            var_cols: var_cols(&[("a", 0)]),
                            computed: vec![Computed {
                                constant: 1,
                                ..sum_of("s", &[])
                            }],
                            yield_vars: vec!["a".to_string(), "s".to_string()],
                            ..OracleClause::default()
                        },
                        read_other("mfb", false),
                        OracleClause {
                            base: fork,
                            var_cols: var_cols(&[("a", 0), ("c", 1), ("w", 2), ("d", 3)]),
                            pos_refs: vec![
                                tuple_ref("mfb", &["c", "t"]),
                                tuple_ref("mfa", &["d", "u"]),
                            ],
                            computed: vec![sum_of("x", &["t", "u", "w"])],
                            yield_vars: vec!["a".to_string(), "x".to_string()],
                            ..OracleClause::default()
                        },
                    ],
                )
            },
            OracleRule {
                fold,
                ..plain_rule("mfb", &cols, 1, vec![read_other("mfa", true)])
            },
        ]);
    }

    if rec.mutual_along {
        stmts.push(clause(
            "mpa",
            "({a}:Node)-[{e}:EDGE]->({b}:Node)",
            vec![filter("{a}.id < {b}.id")],
            "ALONG {mpa:q} = {e}.w",
            vec![key_var("a"), key_var("b"), own_col("mpa", "q")],
        ));
        stmts.push(clause(
            "mpa",
            "({a}:Node)-[{e}:EDGE]->({m}:Node)",
            vec![filter("{a}.id < {m}.id"), is_ref(&["m"], "mpa", Some("b"))],
            "ALONG {mpa:q} = prev.{mpa:q} + {e}.w",
            vec![key_var("a"), key_var("b"), own_col("mpa", "q")],
        ));
        let mut conj = vec![
            filter("{a}.id < {m}.id"),
            is_ref(&["m", "b", "p"], "mpa", None),
        ];
        conj.extend(neg_surface("m"));
        stmts.push(clause(
            "mpb",
            "({a}:Node)-[{e}:EDGE]->({m}:Node)",
            conj,
            "",
            vec![key_var("a"), key_var("b"), item(false, "{p} + {e}.w", "q")],
        ));
        // `mpb` re-weighting an `mpa` path by an edge parallel to it. The hop
        // above needs two chained acyclic edges and the `mpa` clause reading
        // `mpb` a third, which the sparse random graphs rarely hold (`mpb` was
        // non-empty in ~7% of cases); this clause needs one, so `mpb` is
        // non-empty whenever `mpa` is and the mutual cycle `mpa -> mpb -> mpa`
        // turns on any two chained edges.
        stmts.push(clause(
            "mpb",
            "({a}:Node)-[{e}:EDGE]->({b}:Node)",
            vec![
                filter("{a}.id < {b}.id"),
                is_ref(&["a", "b", "p"], "mpa", None),
            ],
            "",
            vec![key_var("a"), key_var("b"), item(false, "{p} + {e}.w", "q")],
        ));
        stmts.push(clause(
            "mpa",
            "({a}:Node)-[{e}:EDGE]->({m}:Node)",
            vec![
                filter("{a}.id < {m}.id"),
                is_ref(&["m", "b", "p"], "mpb", None),
            ],
            "ALONG {mpa:q} = {p} + {e}.w",
            vec![key_var("a"), key_var("b"), own_col("mpa", "q")],
        ));
        let cols = [
            ("a", ColKind::Node),
            ("b", ColKind::Node),
            ("q", ColKind::Int),
        ];
        let hop = |other: &str, negated: bool| OracleClause {
            base: dag.clone(),
            var_cols: edge_cols("a", "m"),
            pos_refs: vec![tuple_ref(other, &["m", "b", "p"])],
            neg_refs: if negated { neg_ir("m") } else { Vec::new() },
            computed: vec![sum_of("q", &["p", "w"])],
            yield_vars: abd("q"),
            ..OracleClause::default()
        };
        strata.push(vec![
            OracleRule {
                per_path: true,
                ..plain_rule(
                    "mpa",
                    &cols,
                    2,
                    vec![
                        OracleClause {
                            base: dag.clone(),
                            var_cols: edge_cols("a", "b"),
                            computed: vec![sum_of("q", &["w"])],
                            yield_vars: abd("q"),
                            ..OracleClause::default()
                        },
                        OracleClause {
                            base: dag.clone(),
                            var_cols: edge_cols("a", "m"),
                            pos_refs: vec![IsRef {
                                values: vec!["p".to_string()],
                                ..isref("mpa", &["m"], Some("b"))
                            }],
                            computed: vec![sum_of("q", &["p", "w"])],
                            yield_vars: abd("q"),
                            ..OracleClause::default()
                        },
                        hop("mpb", false),
                    ],
                )
            },
            plain_rule(
                "mpb",
                &cols,
                2,
                vec![
                    hop("mpa", true),
                    OracleClause {
                        base: dag.clone(),
                        var_cols: edge_cols("a", "b"),
                        pos_refs: vec![tuple_ref("mpa", &["a", "b", "p"])],
                        computed: vec![sum_of("q", &["p", "w"])],
                        yield_vars: abd("q"),
                        ..OracleClause::default()
                    },
                ],
            ),
        ]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Independent transitive closure (distinct from the oracle) for cross-checking.
    fn brute_closure(edges: &[Tuple]) -> HashSet<(i64, i64)> {
        let mut set: HashSet<(i64, i64)> = edges.iter().map(|t| (t[0], t[1])).collect();
        loop {
            let snapshot: Vec<(i64, i64)> = set.iter().copied().collect();
            let mut added = false;
            for &(a, b) in &snapshot {
                for &(c, d) in &snapshot {
                    if b == c && set.insert((a, d)) {
                        added = true;
                    }
                }
            }
            if !added {
                break;
            }
        }
        set
    }

    fn edges_of(g: &Generated) -> &Vec<Tuple> {
        &g.oracle_rules.strata[0][0].clauses[0].base
    }

    #[test]
    fn edge_count_matches_formula() {
        for stages in 1..6 {
            for width in 1..8 {
                let g = build_layered_dag(stages, width);
                assert_eq!(
                    edges_of(&g).len(),
                    width * width * stages.saturating_sub(1),
                    "stages={stages} width={width}"
                );
            }
        }
    }

    #[test]
    fn closure_size_matches_closed_form_and_brute_force() {
        for stages in 1..6 {
            for width in 1..8 {
                let g = build_layered_dag(stages, width);
                let bf = brute_closure(edges_of(&g));
                assert_eq!(
                    bf.len(),
                    expected_closure_size(stages, width),
                    "stages={stages} width={width}"
                );
            }
        }
    }

    #[test]
    fn program_text_has_both_clauses() {
        let g = build_layered_dag(3, 5);
        assert_eq!(g.program_text.matches("CREATE RULE reaches AS").count(), 2);
        assert!(g.program_text.contains("mid IS reaches TO b"));
    }

    #[test]
    fn cypher_has_right_node_and_edge_counts() {
        let g = build_layered_dag(3, 4);
        assert_eq!(g.base_graph_cypher.matches(":Node {id:").count(), 3 * 4);
        assert_eq!(
            g.base_graph_cypher.matches("[:EDGE]").count(),
            4 * 4 * (3 - 1)
        );
        assert!(g.base_graph_cypher.starts_with("CREATE "));
    }

    #[test]
    fn oracle_rules_structure() {
        let g = build_layered_dag(2, 3);
        assert_eq!(g.oracle_rules.strata.len(), 1);
        let rule = &g.oracle_rules.strata[0][0];
        assert_eq!(rule.name, "reaches");
        assert_eq!(rule.clauses.len(), 2);
        let rec = &rule.clauses[1];
        assert_eq!(rec.pos_refs.len(), 1);
        assert_eq!(rec.pos_refs[0].rule, "reaches");
        assert_eq!(rec.pos_refs[0].subjects, vec!["mid".to_string()]);
        assert_eq!(rec.pos_refs[0].target.as_deref(), Some("b"));
        assert_eq!(
            g.key_schema["reaches"],
            vec!["a".to_string(), "b".to_string()]
        );
    }

    // ---- Parity with the hand-written templates the surface replaced ----

    /// The program text exactly as the generator wrote it by hand before the
    /// surface renderer existed. Kept as a compact parity reference: every
    /// legacy template, reached through every legacy shape flag.
    fn legacy_text(shape: &RandomShape) -> String {
        let arrow = |ty: &str| {
            if shape.reverse {
                format!("<-[:{ty}]-")
            } else {
                format!("-[:{ty}]->")
            }
        };
        let mut kinds = vec!["EDGE"];
        if shape.union {
            kinds.push("EDGE2");
        }
        let mut text: Vec<String> = Vec::new();
        for ty in &kinds {
            text.push(format!(
                "CREATE RULE link AS MATCH (a:Node){}(b:Node) YIELD KEY a, KEY b",
                arrow(ty)
            ));
        }
        for ty in &kinds {
            text.push(format!(
                "CREATE RULE reach AS MATCH (a:Node){}(b:Node) YIELD KEY a, KEY b",
                arrow(ty)
            ));
            text.push(format!(
                "CREATE RULE reach AS MATCH (a:Node){}(m:Node) WHERE m IS reach TO b YIELD KEY a, KEY b",
                arrow(ty)
            ));
        }
        if shape.negation {
            text.push(
                "CREATE RULE un AS MATCH (a:Node), (b:Node) WHERE a IS NOT reach TO b YIELD KEY a, KEY b"
                    .to_string(),
            );
        }
        if shape.along {
            text.push(
                "CREATE RULE cost AS MATCH (a:Node)-[e:EDGE]->(b:Node) WHERE a.id < b.id \
                 ALONG q = e.w YIELD KEY a, KEY b, q"
                    .to_string(),
            );
            text.push(
                "CREATE RULE cost AS MATCH (a:Node)-[e:EDGE]->(m:Node) WHERE a.id < m.id, m IS cost TO b \
                 ALONG q = prev.q + e.w YIELD KEY a, KEY b, q"
                    .to_string(),
            );
            text.push(
                "CREATE RULE tot AS MATCH (a:Node) WHERE a IS cost TO b \
                 FOLD n = COUNT(*), s = SUM(q) YIELD KEY a, n, s"
                    .to_string(),
            );
        }
        if shape.rollup {
            text.push("CREATE RULE roll AS MATCH (a:Node) YIELD KEY a, 1 AS s".to_string());
            text.push(
                "CREATE RULE roll AS MATCH (a:Node)-[e:EDGE]->(c:Node) WHERE a.id < c.id, c IS roll \
                 FOLD s = MSUM(s + e.w) YIELD KEY a, s"
                    .to_string(),
            );
        }
        if let Some(ascending) = shape.best {
            let (filter, recursive_filter, order) = if ascending {
                ("", "", "ASC")
            } else {
                (" WHERE a.id < b.id", "a.id < m.id, ", "DESC")
            };
            text.push(format!(
                "CREATE RULE opt AS MATCH (a:Node)-[e:EDGE]->(b:Node){filter} \
                 ALONG d = e.w BEST BY d {order} YIELD KEY a, KEY b, d"
            ));
            text.push(format!(
                "CREATE RULE opt AS MATCH (a:Node)-[e:EDGE]->(m:Node) WHERE {recursive_filter}m IS opt TO b \
                 ALONG d = prev.d + e.w BEST BY d {order} YIELD KEY a, KEY b, d"
            ));
        }
        if shape.folds[0] {
            text.push(
                "CREATE RULE deg AS MATCH (a:Node)-[e:EDGE]->(b:Node) \
                 FOLD n = COUNT(*), s = SUM(e.w), lo = MIN(e.w), hi = MAX(e.w), c = COUNT(e.w) \
                 YIELD KEY a, n, s, lo, hi, c"
                    .to_string(),
            );
        }
        if shape.folds[1] {
            text.push(
                "CREATE RULE span AS MATCH (a:Node) WHERE a IS reach TO b \
                 FOLD n = COUNT(*), hi = MAX(b.id), lo = MMIN(b.id) YIELD KEY a, n, hi, lo"
                    .to_string(),
            );
        }
        if shape.folds[2] {
            text.push(
                "CREATE RULE flow AS MATCH (a:Node)-[e:EDGE]->(m:Node) WHERE m IS reach TO b \
                 FOLD n = COUNT(*), s = MSUM(e.w), k = MCOUNT(b.id) YIELD KEY a, n, s, k"
                    .to_string(),
            );
        }
        if shape.prob {
            text.push(
                "CREATE RULE pr AS MATCH (a:Node)-[e:EDGE]->(b:Node) YIELD KEY a, KEY b, e.w / 10.0 AS pb PROB"
                    .to_string(),
            );
            for (name, function) in [("nor", "MNOR"), ("prd", "MPROD")] {
                text.push(format!(
                    "CREATE RULE {name} AS MATCH (a:Node)-[e:EDGE]->(b:Node) \
                     FOLD pb = {function}(e.w / 10.0) YIELD KEY a, KEY b, pb"
                ));
            }
            text.push(
                "CREATE RULE hop AS MATCH (a:Node)-[e:EDGE]->(m:Node) WHERE m IS nor TO b \
                 FOLD hp = MNOR(e.w / 10.0) YIELD KEY a, KEY b, hp"
                    .to_string(),
            );
            text.push(
                "CREATE RULE safe AS MATCH (a:Node), (b:Node) WHERE a IS NOT nor TO b \
                 YIELD KEY a, KEY b, 1.0 AS pb PROB"
                    .to_string(),
            );
        }
        text.join("\n")
    }

    fn legacy_shape() -> impl Strategy<Value = RandomShape> {
        (
            any::<[bool; 4]>(),
            any::<[bool; 3]>(),
            any::<[bool; 2]>(),
            proptest::option::of(any::<bool>()),
        )
            .prop_map(
                |([reverse, union, negation, along], folds, [rollup, prob], best)| RandomShape {
                    nodes: 3,
                    edges: vec![(0, 1, 2, false), (1, 2, 3, true), (0, 2, 1, false)],
                    reverse,
                    union,
                    negation,
                    folds,
                    along,
                    rollup,
                    best,
                    prob,
                    binding: BindingShape::none(),
                    recursion: RecursionShape::none(),
                },
            )
    }

    proptest! {
        /// The canonical rendering of every legacy shape is byte-identical
        /// to the hand-written templates it replaced.
        #[test]
        fn surface_renders_the_legacy_templates(shape in legacy_shape()) {
            prop_assert_eq!(build_random_program(&shape).program_text, legacy_text(&shape));
        }

        /// Every rendering of a random program is consistent with its oracle
        /// IR: one surface clause per oracle clause, and the same references.
        #[test]
        fn surface_and_oracle_agree(g in random_program_strategy()) {
            surface_matches_oracle(&g);
        }

        /// A renaming of every variable, rule and output column leaves no
        /// original name in the text: no name hides in a literal.
        #[test]
        fn every_name_is_marked(g in random_program_strategy()) {
            every_name_renamed(&g);
        }
    }

    #[test]
    fn fixed_builders_render_their_old_text() {
        const REACHES: &str = concat!(
            "CREATE RULE reaches AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b\n",
            "CREATE RULE reaches AS MATCH (a:Node)-[:EDGE]->(mid:Node) ",
            "WHERE mid IS reaches TO b YIELD KEY a, KEY b",
        );
        assert_eq!(build_layered_dag(3, 2).program_text, REACHES);
        assert_eq!(
            build_complement(3, 2).program_text,
            format!(
                "{REACHES}\nCREATE RULE unreached AS MATCH (a:Node), (b:Node) \
                 WHERE a IS NOT reaches TO b YIELD KEY a, KEY b"
            )
        );
        assert_eq!(
            build_union(2, 2).program_text,
            "CREATE RULE linked AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b\n\
             CREATE RULE linked AS MATCH (a:Node)-[:EDGE2]->(b:Node) YIELD KEY a, KEY b"
        );
    }

    /// Asserts each rule has as many surface clauses as oracle clauses and
    /// that the k-th of each references the same rules, positive and negated.
    fn surface_matches_oracle(g: &Generated) {
        let mut surface: HashMap<&str, Vec<&SurfaceClause>> = HashMap::new();
        for (_, c) in g.surface.clauses() {
            surface.entry(c.head.as_str()).or_default().push(c);
        }
        for rule in g.oracle_rules.strata.iter().flatten() {
            let clauses = &surface[rule.name.as_str()];
            assert_eq!(clauses.len(), rule.clauses.len(), "{}", rule.name);
            for (s, o) in clauses.iter().zip(&rule.clauses) {
                let mut pos: Vec<&str> = s
                    .refs()
                    .filter(|(_, r)| !r.negated)
                    .map(|(_, r)| r.rule.as_str())
                    .collect();
                let mut neg: Vec<&str> = s
                    .refs()
                    .filter(|(_, r)| r.negated)
                    .map(|(_, r)| r.rule.as_str())
                    .collect();
                let mut opos: Vec<&str> = o.pos_refs.iter().map(|r| r.rule.as_str()).collect();
                let mut oneg: Vec<&str> = o
                    .neg_refs
                    .iter()
                    .chain(o.prob_complements.iter().map(|pc| &pc.reference))
                    .map(|r| r.rule.as_str())
                    .collect();
                for v in [&mut pos, &mut neg, &mut opos, &mut oneg] {
                    v.sort_unstable();
                }
                assert_eq!((pos, neg), (opos, oneg), "{}", rule.name);
                assert_eq!(s.yields.len(), o.yield_vars.len().max(s.yields.len()));
            }
        }
    }

    /// Renders `g` with every name marked renamed and checks no unmarked
    /// identifier survives, other than keywords, functions, labels and
    /// property keys.
    fn every_name_renamed(g: &Generated) {
        use crate::surface::{Orders, Renaming};
        let mut renaming = Renaming::identity();
        for (i, c) in g.surface.clauses() {
            let vars = c.vars().into_iter().map(|v| (v.clone(), format!("{v}_R")));
            renaming.vars.insert(i, vars.collect());
            renaming
                .rules
                .insert(c.head.clone(), format!("R_{}", c.head));
            for (r, col) in c.cols() {
                renaming.cols.insert((r, col.clone()), format!("C_{col}"));
            }
            for (_, r) in c.refs() {
                renaming
                    .rules
                    .insert(r.rule.clone(), format!("R_{}", r.rule));
            }
        }
        let text = g.surface.render(&renaming, &Orders::canonical());
        const ALLOWED: &[&str] = &[
            "CREATE", "RULE", "AS", "MATCH", "WHERE", "YIELD", "KEY", "IS", "NOT", "TO", "ALONG",
            "FOLD", "BEST", "BY", "ASC", "DESC", "PROB", "COUNT", "SUM", "MIN", "MAX", "MSUM",
            "MMIN", "MCOUNT", "MNOR", "MPROD", "prev",
        ];
        let bytes = text.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let ch = bytes[i] as char;
            if ch.is_ascii_alphabetic() || ch == '_' {
                let start = i;
                while i < bytes.len()
                    && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                let token = &text[start..i];
                let before = text[..start].chars().next_back();
                // A label (`:Node`) or a property key (`a.id`), but a
                // `prev.col` column must be renamed.
                let property = before == Some('.') && !text[..start - 1].ends_with("prev");
                let marked =
                    token.ends_with("_R") || token.starts_with("R_") || token.starts_with("C_");
                assert!(
                    property || before == Some(':') || marked || ALLOWED.contains(&token),
                    "unmarked name `{token}` in:\n{text}"
                );
            } else if ch.is_ascii_digit() {
                while i < bytes.len()
                    && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'.')
                {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
    }

    #[test]
    fn binding_families_check_and_reach_their_features() {
        let mut seen: BTreeSet<Feature> = BTreeSet::new();
        for entropy in 0..64u64 {
            let shape = RandomShape {
                nodes: 5,
                edges: vec![
                    (0, 1, 2, false),
                    (1, 2, 3, false),
                    (2, 3, 1, true),
                    (0, 3, 4, false),
                    (3, 4, 5, true),
                    (1, 1, 2, false),
                ],
                reverse: false,
                union: false,
                negation: false,
                folds: [false; 3],
                along: false,
                rollup: false,
                best: None,
                prob: false,
                binding: BindingShape {
                    families: [true; 7],
                    entropy,
                },
                recursion: RecursionShape::none(),
            };
            let g = build_random_program(&shape);
            match check_program(&g.oracle_rules) {
                Ok(()) => assert!(!g.features.contains(&Feature::AmbiguousExpected)),
                Err(OracleError::AmbiguousRuleColumn { .. }) => {
                    assert!(g.features.contains(&Feature::AmbiguousExpected));
                }
                Err(e) => panic!("{e}\n{}", g.program_text),
            }
            surface_matches_oracle(&g);
            seen.extend(g.features.iter().copied());
        }
        for f in [
            Feature::MultiRef,
            Feature::RefBoundKey,
            Feature::RefBoundValue,
            Feature::RefBoundTuple,
            Feature::RefBoundTo,
            Feature::NameCollision,
            Feature::SharedValueName,
            Feature::AmbiguousExpected,
            Feature::PrevBesideValue,
            Feature::ScalarKeyInt,
            Feature::ScalarKeyStr,
            Feature::NegBeforeBinder,
            Feature::NegAfterBinder,
            Feature::NegPrefixForm,
            Feature::NegSuffixForm,
        ] {
            assert!(seen.contains(&f), "no case reached {f:?}");
        }
    }
}
