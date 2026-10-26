// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Query-rewrite relations over a graph shaped the way real bugs need.
//!
//! The DQP levers compare one query under two engine states, over a bipartite
//! one-hop fixture with no parallel edges, diamonds or cycles, and `querygen`
//! never emits a variable-length relationship, a named relationship, `OPTIONAL
//! MATCH`, `DISTINCT` or a subquery. Every silent wrong answer found on
//! 2026-09-28 needed at least one of those (see
//! `docs/proposals/correctness_class_harness_2026-09-28.md`, W3).
//!
//! This module holds the engine and its data fixed and varies the **query**
//! instead: each [`Relation`] renders a generated [`TopoCase`] as one query and
//! as one or more equivalent formulations, and the bag of the first must equal
//! the multiset union of each formulation's parts. The oracle needs no
//! reference implementation — only a Cypher identity:
//!
//! | relation | identity |
//! |---|---|
//! | `named_rel` | an unreferenced relationship variable changes nothing |
//! | `optional` | `MATCH (a) OPTIONAL MATCH p` = `MATCH (a) MATCH p` ⊎ `MATCH (a) WHERE NOT EXISTS { MATCH p }` with `p`'s variables NULL |
//! | `exists` | `EXISTS { MATCH p }` = `COUNT { MATCH p } > 0` = the pattern predicate `p` |
//! | `comprehension` | `size([p \| 1])` = `COUNT { MATCH p }` |
//! | `unroll` | `-[*i..j]-` = ⊎ₖ `-[*k..k]-` = ⊎ₖ k fixed hops |
//! | `distinct` | `RETURN DISTINCT x` = `WITH x, count(*) AS n RETURN x` |
//! | `aggregate` | `count`/`min`/`max`/`sum`/`avg` = `reduce` over `collect` |
//! | `unwind` | `UNWIND [v…] AS u … WHERE a.id = u` = ⊎ᵥ the query with `u := v` |
//! | `collect_unwind` | `UNWIND collect(x)` = the rows where `x` is not NULL |
//! | `locy_rule` | a Locy rule's facts = the `DISTINCT` rows of its body |
//! | `locy_fold` | `FOLD COUNT/SUM/MIN/MAX` = the Cypher aggregate grouped by the `KEY` |
//! | `locy_reach` | a recursive reachability rule = `-[*1..]->` with `DISTINCT` |
//! | `locy_negation` | `a IS NOT reach TO b` = `NOT EXISTS { (a)-[*1..]->(b) }` |
//! | `locy_query_where` | a `QUERY ... WHERE` filter = the same filter in the rule body |
//! | `locy_commands` | an `EXPLAIN RULE` or `ABDUCE` before a `QUERY` leaves its answer unchanged |
//! | `locy_binding` | a rule's value read by tuple, by `TO`, by name, and through a permuted `YIELD` |
//! | `locy_permute_conjuncts` | a reader's IS references in any order, joined by `,` or `AND`, derive the same facts; reading a value column two references expose is a compile error in every order |
//! | `locy_alpha_rename` | renaming the reader's variables, the rules, and their value columns changes nothing |
//! | `locy_rename_into_collision` | renaming a reader variable onto a value column *another* referenced rule exposes changes nothing (the variable shadows the column) |
//! | `locy_explain_agrees` | the facts `EXPLAIN RULE` derives are the facts evaluation returns |
//!
//! The four `locy_*` relations below `locy_binding` render their readers from a
//! [`LocyText`] template, so the same program is produced under any conjunct
//! order and any renaming (issue #300: the binding of IS references depended
//! on both).
//!
//! The fixture ([`build_topo`]) has chains, a diamond, parallel edges (two with
//! identical properties), self-loops, a 2-cycle and a 3-cycle, fan-in, fan-out,
//! isolated nodes, a cycle through a second label, and a dense cluster. Its
//! [`Layout::HalfUnflushed`] variant leaves part of the graph in L0.
//!
//! Every reference query also runs twice and must return the same bag, so an
//! order-dependent operator is reported as such rather than as a flaky
//! disagreement between formulations.
//!
//! **Activation.** A rewrite moves no execution counter, so a case counts as
//! activated for a relation when the relation applies to it *and* the reference
//! query returns rows: an empty bag agrees with anything and proves nothing.
//! Each relation is held to [`MIN_NON_EMPTY_RATE`].
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(/metamorphic::dqp::topo::/)'

// Rust guideline compliant

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use proptest::prelude::*;
use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestRng, TestRunner};
use uni_db::{DataType, Uni, UniConfig};

use crate::diff::{RowBag, bag, bag_eq, bag_union};

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// Where the fixture's rows live when the relations run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    /// Everything flushed to storage.
    Flushed,
    /// Nodes and the first half of the edges flushed; the rest of the edges,
    /// and a few more nodes, still in L0.
    HalfUnflushed,
}

/// `Person.id` values, one per node.
const PERSONS: std::ops::Range<i64> = 0..48;

/// `Robot.id` values: a second label on the same relationship type.
const ROBOTS: std::ops::Range<i64> = 100..104;

/// Nodes created only after the flush in [`Layout::HalfUnflushed`].
const LATE_PERSONS: std::ops::Range<i64> = 48..52;

/// Anchor ids an `UNWIND` draws from: mostly nodes with edges, plus an
/// isolated one, a robot (not a `Person`), and an id no node has.
const UNWIND_IDS: [i64; 12] = [0, 1, 5, 9, 12, 13, 16, 18, 31, 36, 40, 100];

/// `age` values cycle through this domain; `None` is a missing property.
const AGE_DOMAIN: [Option<i64>; 4] = [Some(10), Some(20), Some(30), None];

/// Integer literals predicates draw from: present ids and ages, plus values no
/// row holds.
const LITERALS: [i64; 10] = [0, 3, 9, 10, 15, 20, 30, 40, 100, 102];

fn age_of(id: i64) -> Option<i64> {
    AGE_DOMAIN[(id.rem_euclid(AGE_DOMAIN.len() as i64)) as usize]
}

fn label_of(id: i64) -> &'static str {
    if ROBOTS.contains(&id) {
        "Robot"
    } else {
        "Person"
    }
}

/// Every `KNOWS` edge as `(src id, dst id, w)`, in creation order.
fn edges() -> Vec<(i64, i64, Option<i64>)> {
    let mut e: Vec<(i64, i64, Option<i64>)> = Vec::new();
    // A chain.
    e.extend([
        (0, 1, Some(1)),
        (1, 2, Some(2)),
        (2, 3, Some(3)),
        (3, 4, Some(4)),
    ]);
    // A diamond: two equal-length paths 5 -> 8.
    e.extend([
        (5, 6, Some(1)),
        (5, 7, Some(2)),
        (6, 8, Some(1)),
        (7, 8, Some(2)),
    ]);
    // Parallel edges; the first and last carry identical properties.
    e.extend([(9, 10, Some(1)), (9, 10, Some(2)), (9, 10, Some(1))]);
    // Self-loops, one of them doubled.
    e.extend([(11, 11, Some(1)), (12, 12, Some(1)), (12, 12, None)]);
    // A 3-cycle and a 2-cycle.
    e.extend([(13, 14, Some(1)), (14, 15, Some(2)), (15, 13, Some(3))]);
    e.extend([(16, 17, Some(1)), (17, 16, None)]);
    // Fan-out and fan-in.
    e.extend((19..24).map(|d| (18, d, Some(d % 3))));
    e.extend((24..29).map(|s| (s, 29, Some(s % 3))));
    // 30..36 stay isolated. A cycle through the second label, a robot
    // self-loop, and a robot reached from the chain.
    e.extend([(0, 100, Some(1)), (100, 101, Some(2)), (101, 0, Some(3))]);
    e.extend([(102, 102, None), (3, 103, Some(1))]);
    // A dense deterministic cluster over 36..48.
    for i in 0..24_i64 {
        let s = 36 + (i * 5) % 12;
        let d = 36 + (i * 7 + 3) % 12;
        e.push((s, d, (i % 5 != 0).then_some(i % 3)));
    }
    e
}

/// Edges touching the nodes created late, all created after the flush.
fn late_edges() -> Vec<(i64, i64, Option<i64>)> {
    vec![
        (48, 49, Some(1)),
        (49, 50, Some(2)),
        (50, 48, None),
        (51, 51, Some(1)),
        (4, 48, Some(3)),
        (48, 0, Some(2)),
    ]
}

fn node_literal(id: i64) -> String {
    match age_of(id) {
        Some(age) => format!("(:{} {{id: {id}, age: {age}}})", label_of(id)),
        None => format!("(:{} {{id: {id}}})", label_of(id)),
    }
}

/// One `CREATE` per `(src label, dst label)` pair, each over an `UNWIND`ed list.
fn edge_statements(edges: &[(i64, i64, Option<i64>)]) -> Vec<String> {
    let mut by_labels: BTreeMap<(&str, &str), Vec<String>> = BTreeMap::new();
    for &(s, d, w) in edges {
        let w = w.map_or_else(|| "null".to_string(), |w| w.to_string());
        by_labels
            .entry((label_of(s), label_of(d)))
            .or_default()
            .push(format!("{{s: {s}, d: {d}, w: {w}}}"));
    }
    by_labels
        .into_iter()
        .map(|((sl, dl), rows)| {
            format!(
                "UNWIND [{}] AS e MATCH (s:{sl} {{id: e.s}}), (d:{dl} {{id: e.d}}) \
                 CREATE (s)-[:KNOWS {{w: e.w}}]->(d)",
                rows.join(", ")
            )
        })
        .collect()
}

async fn run_all(db: &Uni, statements: &[String]) -> anyhow::Result<()> {
    let session = db.session();
    let tx = session.tx().await?;
    for statement in statements {
        tx.execute(statement).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Builds the topology fixture.
///
/// # Errors
///
/// Returns an error if the database cannot be opened or a statement fails.
pub async fn build_topo(
    layout: Layout,
    execution_batch_size: Option<usize>,
) -> anyhow::Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            execution_batch_size,
            // The per-query deadline is not what this harness measures, and the
            // default 30 s one fails it spuriously under load. The `locy_negation`
            // reference `MATCH (a:Person), (b:Person) WHERE NOT EXISTS { MATCH
            // (a)-[:KNOWS*1..]->(b) }` once evaluated its unbounded VLP per pair
            // (52 x 52), measured at 5.9 s alone with a two-row batch; with the
            // machine oversubscribed (36 concurrent copies plus a build) it crossed
            // 30 s in 34 of 36 runs and surfaced as a "divergence". It now runs
            // once per source (0.29 s, debug build), but the deadline is still
            // not what this harness measures. A hang or a runaway rewrite is
            // still caught, by the six-minute nextest limit on these tests
            // (`.config/nextest.toml`).
            query_timeout: std::time::Duration::from_secs(300),
            // The test decides what is flushed.
            auto_flush_interval: None,
            auto_flush_threshold: usize::MAX,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("Person")
        .property("id", DataType::Int)
        .property_nullable("age", DataType::Int)
        .label("Robot")
        .property("id", DataType::Int)
        .property_nullable("age", DataType::Int)
        .edge_type("KNOWS", &["Person", "Robot"], &["Person", "Robot"])
        .property_nullable("w", DataType::Int)
        .done()
        .apply()
        .await?;

    let nodes: Vec<String> = PERSONS.chain(ROBOTS).map(node_literal).collect();
    run_all(&db, &[format!("CREATE {}", nodes.join(", "))]).await?;

    let all = edges();
    match layout {
        Layout::Flushed => {
            run_all(&db, &edge_statements(&all)).await?;
            db.flush().await?;
        }
        Layout::HalfUnflushed => {
            let (early, late) = all.split_at(all.len() / 2);
            run_all(&db, &edge_statements(early)).await?;
            db.flush().await?;
            let late_nodes: Vec<String> = LATE_PERSONS.map(node_literal).collect();
            run_all(&db, &[format!("CREATE {}", late_nodes.join(", "))]).await?;
            let mut rest = late.to_vec();
            rest.extend(late_edges());
            run_all(&db, &edge_statements(&rest)).await?;
        }
    }
    Ok(db)
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// A pattern variable a predicate or projection can name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Var {
    /// The anchor, `(a:Person)`, bound before the pattern.
    A,
    /// The pattern's far end.
    B,
}

impl Var {
    fn name(self) -> &'static str {
        match self {
            Var::A => "a",
            Var::B => "b",
        }
    }
}

/// A predicate over `id` and `age`.
#[derive(Clone, Debug)]
pub enum Pred {
    /// `v.prop op literal`.
    Cmp(Var, &'static str, &'static str, i64),
    /// `v.prop IS NULL`.
    IsNull(Var, &'static str),
    /// Conjunction.
    And(Box<Pred>, Box<Pred>),
    /// Disjunction.
    Or(Box<Pred>, Box<Pred>),
    /// Negation.
    Not(Box<Pred>),
}

impl Pred {
    fn render(&self) -> String {
        match self {
            Pred::Cmp(v, p, op, lit) => format!("({}.{p} {op} {lit})", v.name()),
            Pred::IsNull(v, p) => format!("({}.{p} IS NULL)", v.name()),
            Pred::And(l, r) => format!("({} AND {})", l.render(), r.render()),
            Pred::Or(l, r) => format!("({} OR {})", l.render(), r.render()),
            Pred::Not(p) => format!("(NOT {})", p.render()),
        }
    }
}

/// A relationship's direction, read from `a` toward `b`.
#[derive(Clone, Copy, Debug)]
pub enum Dir {
    /// `-[..]->`
    Out,
    /// `<-[..]-`
    In,
    /// `-[..]-`
    Both,
}

/// One relationship of the pattern.
#[derive(Clone, Copy, Debug)]
pub struct Hop {
    dir: Dir,
    /// `Some((i, j))` renders `*i..j`; `None` is a single fixed hop.
    range: Option<(u32, u32)>,
}

/// The label constraint on `b`.
#[derive(Clone, Copy, Debug)]
pub enum EndLabel {
    /// `(b:Person)`
    Person,
    /// `(b:Robot)`
    Robot,
    /// `(b:Person|Robot)`
    Either,
    /// `(b)`
    Any,
}

impl EndLabel {
    fn render(self) -> &'static str {
        match self {
            EndLabel::Person => ":Person",
            EndLabel::Robot => ":Robot",
            EndLabel::Either => ":Person|Robot",
            EndLabel::Any => "",
        }
    }
}

/// A projected column.
#[derive(Clone, Copy, Debug)]
pub struct Item {
    var: Var,
    prop: &'static str,
}

/// A generated case: `(a:Person)` bound, a one- or two-hop `KNOWS` pattern from
/// it to `b`, and predicates and projections over both ends.
#[derive(Clone, Debug)]
pub struct TopoCase {
    anchor: Option<Pred>,
    hops: Vec<Hop>,
    end: EndLabel,
    inner: Option<Pred>,
    items: Vec<Item>,
    /// Anchor ids an `UNWIND` feeds in, duplicates allowed.
    unwind: Vec<i64>,
    /// A permutation of `0..3`: the order a Locy reader lists its conjuncts in
    /// (each reader takes the order it induces on its own conjuncts).
    perm: Vec<usize>,
    /// Picks the renaming and the collisions the Locy relations apply.
    renaming: usize,
}

/// ` WHERE c1 AND c2 ...`, or nothing when there are no conditions.
fn where_all(conds: impl Iterator<Item = String>) -> String {
    let conds: Vec<String> = conds.collect();
    if conds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conds.join(" AND "))
    }
}

/// A recursive Locy reachability rule over every node, along `hop`'s
/// direction.
fn reach_rules(hop: Hop) -> String {
    let rel = render_rel(hop, None, None);
    format!(
        "CREATE RULE reach AS MATCH (a){rel}(b) YIELD KEY a, KEY b \
         CREATE RULE reach AS MATCH (a){rel}(m) WHERE m IS reach TO b YIELD KEY a, KEY b"
    )
}

/// How relationships are written when a pattern is rendered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Naming {
    Anonymous,
    Named,
}

fn render_rel(hop: Hop, range: Option<(u32, u32)>, name: Option<String>) -> String {
    let name = name.unwrap_or_default();
    let range = match range {
        Some((i, j)) => format!("*{i}..{j}"),
        None => String::new(),
    };
    let body = format!("[{name}:KNOWS{range}]");
    match hop.dir {
        Dir::Out => format!("-{body}->"),
        Dir::In => format!("<-{body}-"),
        Dir::Both => format!("-{body}-"),
    }
}

impl TopoCase {
    /// The pattern from an already-bound `a`, with hop `unroll.0` replaced by
    /// `unroll.1` (a range, or `k` fixed hops when `Err(k)`).
    fn pattern_with(
        &self,
        naming: Naming,
        b_named: bool,
        unroll: Option<(usize, Result<(u32, u32), u32>)>,
    ) -> String {
        let mut out = String::from("(a)");
        let mut rel_no = 0;
        let mut next_name = |naming: Naming| {
            let name = (naming == Naming::Named).then(|| format!("r{rel_no}"));
            rel_no += 1;
            name
        };
        for (i, hop) in self.hops.iter().enumerate() {
            match unroll.filter(|(at, _)| *at == i).map(|(_, how)| how) {
                Some(Err(k)) => {
                    for step in 0..k {
                        out.push_str(&render_rel(*hop, None, next_name(naming)));
                        if step + 1 < k {
                            out.push_str("()");
                        }
                    }
                }
                Some(Ok(range)) => out.push_str(&render_rel(*hop, Some(range), next_name(naming))),
                None => out.push_str(&render_rel(*hop, hop.range, next_name(naming))),
            }
            if i + 1 < self.hops.len() {
                out.push_str("()");
            }
        }
        let b = if b_named { "b" } else { "" };
        let _ = write!(out, "({b}{})", self.end.render());
        out
    }

    fn pattern(&self, naming: Naming) -> String {
        self.pattern_with(naming, true, None)
    }

    fn anchor_where(&self) -> String {
        self.anchor
            .as_ref()
            .map(|p| format!(" WHERE {}", p.render()))
            .unwrap_or_default()
    }

    fn inner_where(&self) -> String {
        self.inner
            .as_ref()
            .map(|p| format!(" WHERE {}", p.render()))
            .unwrap_or_default()
    }

    fn items(&self, null_b: bool) -> String {
        self.items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                if null_b && item.var == Var::B {
                    format!("null AS c{i}")
                } else {
                    format!("{}.{} AS c{i}", item.var.name(), item.prop)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn a_items(&self) -> String {
        let a: Vec<String> = self
            .items
            .iter()
            .filter(|i| i.var == Var::A)
            .enumerate()
            .map(|(n, i)| format!("a.{} AS c{n}", i.prop))
            .collect();
        if a.is_empty() {
            "a.id AS c0".to_string()
        } else {
            a.join(", ")
        }
    }

    /// `MATCH (a:Person) MATCH p WHERE anchor AND inner`, the join of both ends.
    fn joined(&self, naming: Naming) -> String {
        let conds: Vec<String> = self
            .anchor
            .iter()
            .chain(self.inner.iter())
            .map(Pred::render)
            .collect();
        let filter = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        format!("MATCH (a:Person) MATCH {}{filter}", self.pattern(naming))
    }

    fn subquery(&self, pattern: &str) -> String {
        format!("{{ MATCH {pattern}{} }}", self.inner_where())
    }

    fn with_anchor(&self, extra: &str) -> String {
        match &self.anchor {
            Some(p) => format!(" WHERE {} AND {extra}", p.render()),
            None => format!(" WHERE {extra}"),
        }
    }

    /// The pattern as a Locy rule body: the anchor is labelled in place.
    fn locy_pattern(&self) -> String {
        let pattern = self.pattern(Naming::Anonymous);
        format!("(a:Person){}", &pattern["(a)".len()..])
    }

    /// The anchor and inner predicates as one `WHERE`, or nothing.
    fn body_where(&self) -> String {
        where_all(
            self.anchor
                .iter()
                .chain(self.inner.iter())
                .map(Pred::render),
        )
    }

    /// The single ranged hop, if exactly one hop is ranged.
    fn ranged_hop(&self) -> Option<(usize, (u32, u32))> {
        let mut ranged = self
            .hops
            .iter()
            .enumerate()
            .filter_map(|(i, h)| h.range.map(|r| (i, r)));
        let first = ranged.next()?;
        ranged.next().is_none().then_some(first)
    }
}

fn arb_var() -> impl Strategy<Value = Var> {
    prop_oneof![Just(Var::A), Just(Var::B)]
}

fn arb_atom(vars: BoxedStrategy<Var>) -> impl Strategy<Value = Pred> {
    let prop = prop_oneof![Just("id"), Just("age")];
    let op = prop_oneof![
        Just("<"),
        Just("<="),
        Just("="),
        Just("<>"),
        Just(">="),
        Just(">")
    ];
    prop_oneof![
        4 => (vars.clone(), prop.clone(), op, proptest::sample::select(&LITERALS[..]))
            .prop_map(|(v, p, op, lit)| Pred::Cmp(v, p, op, lit)),
        1 => (vars, Just("age")).prop_map(|(v, p)| Pred::IsNull(v, p)),
    ]
}

fn arb_pred(vars: BoxedStrategy<Var>) -> impl Strategy<Value = Pred> {
    let atom = arb_atom(vars).boxed();
    atom.clone().prop_recursive(2, 6, 2, move |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(l, r)| Pred::And(Box::new(l), Box::new(r))),
            (inner.clone(), inner.clone()).prop_map(|(l, r)| Pred::Or(Box::new(l), Box::new(r))),
            inner.prop_map(|p| Pred::Not(Box::new(p))),
        ]
    })
}

fn arb_hop() -> impl Strategy<Value = Hop> {
    let dir = prop_oneof![3 => Just(Dir::Out), 1 => Just(Dir::In), 1 => Just(Dir::Both)];
    let range = prop_oneof![
        3 => Just(None),
        1 => Just(Some((1, 2))),
        1 => Just(Some((1, 3))),
        1 => Just(Some((2, 2))),
        1 => Just(Some((2, 3))),
    ];
    (dir, range).prop_map(|(dir, range)| Hop { dir, range })
}

fn arb_end() -> impl Strategy<Value = EndLabel> {
    prop_oneof![
        3 => Just(EndLabel::Person),
        1 => Just(EndLabel::Robot),
        1 => Just(EndLabel::Either),
        2 => Just(EndLabel::Any),
    ]
}

fn arb_item() -> impl Strategy<Value = Item> {
    (arb_var(), prop_oneof![Just("id"), Just("age")]).prop_map(|(var, prop)| Item { var, prop })
}

/// Generated cases. At most one hop of a two-hop pattern is ranged, so
/// path counts stay small over the dense cluster.
pub fn arb_topo_case() -> impl Strategy<Value = TopoCase> {
    let hops = prop_oneof![
        2 => arb_hop().prop_map(|h| vec![h]),
        1 => (arb_hop(), arb_hop()).prop_map(|(h1, mut h2)| {
            if h1.range.is_some() {
                h2.range = None;
            }
            vec![h1, h2]
        }),
    ];
    (
        proptest::option::weighted(0.5, arb_pred(Just(Var::A).boxed())),
        hops,
        arb_end(),
        proptest::option::weighted(0.4, arb_pred(Just(Var::B).boxed())),
        proptest::collection::vec(arb_item(), 1..=3),
        proptest::collection::vec(proptest::sample::select(&UNWIND_IDS[..]), 1..=4),
        Just(vec![0, 1, 2]).prop_shuffle(),
        0..LOCY_RENAMINGS.len(),
    )
        .prop_map(
            |(anchor, hops, end, inner, items, unwind, perm, renaming)| TopoCase {
                anchor,
                hops,
                end,
                inner,
                items,
                unwind,
                perm,
                renaming,
            },
        )
}

// ---------------------------------------------------------------------------
// Locy reader templates
// ---------------------------------------------------------------------------

/// Placeholder names a [`LocyText`] substitutes.
///
/// Reader variables `a`, `b`, `v`; the reader rule `q`; referenced rules `s`,
/// `p`, `p2`, `f`; and their value columns `k` (of `s`), `n` (of `p` and
/// `p2`), and `m` (of `p2` and `f`). Rule bodies keep their own literal
/// variables: those are local to each rule, so a renaming need not touch
/// them.
const PLACEHOLDERS: [&str; 11] = ["a", "b", "v", "q", "s", "p", "p2", "f", "k", "n", "m"];

/// Placeholder → concrete name; a placeholder absent from the map keeps its
/// own name.
type Names = BTreeMap<&'static str, &'static str>;

/// Consistent renamings for `locy_alpha_rename`, each a bijection on the
/// placeholders it mentions.
const LOCY_RENAMINGS: [&[(&str, &str)]; 3] = [
    // Fresh names for everything the reader and its rules expose.
    &[
        ("a", "x"),
        ("b", "y"),
        ("v", "w"),
        ("q", "out"),
        ("s", "seed"),
        ("p", "pairs"),
        ("k", "kk"),
        ("n", "cnt"),
    ],
    // Swap the reader's two node variables.
    &[("a", "b"), ("b", "a")],
    // Swap the variables, the two rules' names, and their value columns.
    &[
        ("a", "b"),
        ("b", "a"),
        ("s", "p"),
        ("p", "s"),
        ("k", "n"),
        ("n", "k"),
    ],
];

/// Renamings for `locy_rename_into_collision` over the two-reference reader
/// [`p1_reader`]: a variable named like the value column `k` that `s` exposes
/// while `p` binds the variable.
const P1_COLLISIONS: [&[(&str, &str)]; 3] = [
    // A KEY subject `p` binds, named like `s.k`.
    &[("b", "k")],
    // The value `p` binds positionally, named like `s.k`.
    &[("v", "k")],
    // The MATCH variable, named like `s.k`.
    &[("a", "k")],
];

/// Renamings for `locy_rename_into_collision` over the three-reference reader
/// [`three_ref_reader`]: a variable named like the value column `m` that both
/// `p2` and `f` expose.
const THREE_REF_COLLISIONS: [&[(&str, &str)]; 2] = [
    // The value `p2` binds positionally, named like `m`.
    &[("v", "m")],
    // A KEY subject `p2` binds, named like `m`.
    &[("b", "m")],
];

fn names(pairs: &[(&'static str, &'static str)]) -> Names {
    pairs.iter().copied().collect()
}

/// Replaces each `{placeholder}` in `text` by its name in `names`.
///
/// Braces around anything other than a known placeholder are left as they
/// are, so map literals and `COUNT { ... }` pass through.
fn substitute(text: &str, names: &Names) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let token = after.split('}').next().filter(|t| after.len() > t.len());
        match token.and_then(|t| PLACEHOLDERS.iter().find(|p| **p == t)) {
            Some(placeholder) => {
                out.push_str(names.get(placeholder).copied().unwrap_or(placeholder));
                rest = &after[placeholder.len() + 1..];
            }
            None => {
                out.push('{');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// A Locy program ending in one reader rule, as a template.
///
/// Renders `{rules}CREATE RULE {q} AS MATCH {matches} WHERE c₁, c₂, … {tail}`
/// with the conjuncts in a chosen order and every `{placeholder}` replaced by
/// a chosen name, so one template yields every formulation a relation
/// compares.
#[derive(Clone, Debug)]
pub struct LocyText {
    /// The rules the reader references.
    rules: String,
    /// The reader's `MATCH` patterns.
    matches: String,
    /// The reader's `WHERE` conjuncts.
    conjuncts: Vec<String>,
    /// Everything after the reader's `WHERE`: `YIELD`, commands, `QUERY`.
    tail: String,
}

impl LocyText {
    /// Renders with the conjuncts in the order `order` induces on them (its
    /// elements below the conjunct count), joined by `,`.
    fn render(&self, order: &[usize], names: &Names) -> String {
        self.render_joined(order, names, ", ")
    }

    /// [`Self::render`] with an explicit conjunct separator.
    ///
    /// # Panics
    ///
    /// Panics if `order` does not induce a permutation of the conjuncts.
    fn render_joined(&self, order: &[usize], names: &Names, joiner: &str) -> String {
        let induced: Vec<usize> = order
            .iter()
            .copied()
            .filter(|&i| i < self.conjuncts.len())
            .collect();
        let mut check = induced.clone();
        check.sort_unstable();
        assert_eq!(
            check,
            (0..self.conjuncts.len()).collect::<Vec<_>>(),
            "order {order:?} does not permute {} conjuncts",
            self.conjuncts.len()
        );
        let body: Vec<&str> = induced
            .iter()
            .map(|&i| self.conjuncts[i].as_str())
            .collect();
        substitute(
            &format!(
                "{}CREATE RULE {{q}} AS MATCH {} WHERE {} {}",
                self.rules,
                self.matches,
                body.join(joiner),
                self.tail
            ),
            names,
        )
    }

    /// This template with a different tail.
    fn with_tail(&self, tail: impl Into<String>) -> Self {
        Self {
            tail: tail.into(),
            ..self.clone()
        }
    }
}

/// The identity order for up to three conjuncts.
const IDENTITY: [usize; 3] = [0, 1, 2];

/// The `RETURN` every Locy binding reader ends with.
const READER_TAIL: &str = "YIELD KEY {a}, KEY {b}, {v} \
     QUERY {q} RETURN {a}.id AS c0, {b}.id AS c1, {v} AS c2";

/// `s`: every even-id Person, exposing the integer value column `k` (equal to
/// the node's VID in this fixture, see `topo_fixture_has_its_shapes`).
const SEED_RULE: &str = "CREATE RULE {s} AS MATCH (a:Person) WHERE a.id % 2 = 0 \
     YIELD KEY a, a.id AS {k} ";

/// `f`: each Person's degree, exposing `m`.
const DEGREE_RULE: &str =
    "CREATE RULE {f} AS MATCH (a:Person)-[:KNOWS]-(c) FOLD {m} = COUNT(*) YIELD KEY a, {m} ";

impl TopoCase {
    /// `p`: the case's pattern counted per `(a, b)`, exposing `n`.
    fn pair_rule(&self) -> String {
        format!(
            "CREATE RULE {{p}} AS MATCH {}{} FOLD {{n}} = COUNT(*) YIELD KEY a, KEY b, {{n}} ",
            self.locy_pattern(),
            self.body_where()
        )
    }

    /// `p2`: `p` plus a second value column `m`, which `f` exposes too.
    fn pair_rule2(&self) -> String {
        format!(
            "CREATE RULE {{p2}} AS MATCH {}{} FOLD {{n}} = COUNT(*), {{m}} = MIN(b.id) \
             YIELD KEY a, KEY b, {{n}}, {{m}} ",
            self.locy_pattern(),
            self.body_where()
        )
    }
}

/// The #300 shape: a seed test and a reference that binds `b` and `v`, so
/// the reference, not the `MATCH`, is `b`'s binder.
///
/// The canonical order lists the binding reference first, the order that
/// evaluated correctly before the #300 fix; with the seed test first, that
/// build failed to plan (`No field named b`). A reference that already works
/// there lets the other formulations show their divergence against it.
fn p1_reader(case: &TopoCase) -> LocyText {
    LocyText {
        rules: format!("{SEED_RULE}{}", case.pair_rule()),
        matches: "({a}:Person)".to_string(),
        conjuncts: vec![
            "({a}, {b}, {v}) IS {p}".to_string(),
            "{a} IS {s}".to_string(),
        ],
        tail: READER_TAIL.to_string(),
    }
}

/// Three references. `p2` and `f` both expose `m`, which the reader does not
/// read, so the clause is legal; `s` exposes `k`. Every Person with a
/// `KNOWS` edge is in `f`, and every `a` of `p2` has one, so `{a} IS {f}`
/// keeps every row and the facts equal [`p1_reader`]'s.
fn three_ref_reader(case: &TopoCase) -> LocyText {
    LocyText {
        rules: format!("{SEED_RULE}{}{DEGREE_RULE}", case.pair_rule2()),
        matches: "({a}:Person)".to_string(),
        conjuncts: vec![
            "({a}, {b}, {v}) IS {p2}".to_string(),
            "{a} IS {s}".to_string(),
            "{a} IS {f}".to_string(),
        ],
        tail: READER_TAIL.to_string(),
    }
}

/// Prefix of a formulation that runs `EXPLAIN RULE` instead of a `QUERY`; see
/// [`explain_part`].
const EXPLAIN_MARK: &str = "#explain-facts ";

/// A formulation whose rows are the facts `EXPLAIN RULE` derives.
///
/// `keys` are the reader's KEY variables, returned as `c0, c1, …` (their
/// nodes' `id`); a fact counts as derived when its derivation has a child for
/// each rule in `refs`. The rows are a set, like `RETURN DISTINCT`.
fn explain_part(keys: &[&str], refs: &[&str], program: &str) -> String {
    format!(
        "{EXPLAIN_MARK}{};{}\n{program}",
        keys.join(","),
        refs.join(",")
    )
}

// ---------------------------------------------------------------------------
// Relations
// ---------------------------------------------------------------------------

/// A reference query and its equivalent formulations.
///
/// Each formulation is a list of parts whose bags are summed.
#[derive(Debug)]
pub struct Rewrite {
    /// The reference query.
    pub reference: String,
    /// Formulations that must each return the reference's bag.
    pub equivalents: Vec<Vec<String>>,
}

/// A Cypher identity between query formulations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Relation {
    /// An unreferenced relationship variable changes nothing.
    NamedRel,
    /// `OPTIONAL MATCH` is `MATCH` plus the NULL-padded misses.
    Optional,
    /// `EXISTS {}` = `COUNT {} > 0` = the pattern predicate.
    Exists,
    /// `size([pattern | 1])` = `COUNT { MATCH pattern }`.
    Comprehension,
    /// A ranged relationship is the sum of its fixed lengths.
    Unroll,
    /// `DISTINCT` is grouping.
    Distinct,
    /// An aggregate is a `reduce` over the `collect` of the same values.
    Aggregate,
    /// `UNWIND` of a list is the sum over its elements, duplicates included.
    Unwind,
    /// `UNWIND collect(x)` returns the non-null `x` rows.
    CollectUnwind,
    /// A Locy rule's facts are the `DISTINCT` rows of its body as a query.
    LocyRule,
    /// A Locy `FOLD` is the Cypher aggregate grouped by its `KEY`.
    LocyFold,
    /// A recursive Locy reachability rule is a variable-length `DISTINCT` match.
    LocyReach,
    /// `a IS NOT reach TO b` is `NOT EXISTS` of the variable-length match.
    LocyNegation,
    /// A filter in `QUERY ... WHERE` is the same filter in the rule body.
    LocyQueryWhere,
    /// A command never changes what a later command reads: `EXPLAIN RULE` or
    /// `ABDUCE` placed before a `QUERY` leaves the `QUERY`'s answer unchanged.
    LocyCommands,
    /// One binding rule for IS references: reading a rule's value column as a
    /// third subject, as a `TO` target, by its name, or from a copy of the rule
    /// whose `YIELD` lists the value first and the KEYs swapped, derives the
    /// same facts.
    LocyBinding,
    /// A reader's IS references in any order, and joined by `AND` instead of
    /// `,`, derive the same facts; a read of a column two references expose is
    /// a compile error in every order.
    LocyPermuteConjuncts,
    /// A consistent renaming of the reader's variables, the rules it reads,
    /// and their value columns derives the same facts.
    LocyAlphaRename,
    /// Naming a reader variable like a value column another referenced rule
    /// exposes derives the same facts: the variable shadows the column.
    LocyRenameIntoCollision,
    /// `EXPLAIN RULE` derives exactly the facts evaluation returns.
    LocyExplainAgrees,
}

impl Relation {
    /// Every relation, in report order.
    pub const ALL: [Relation; 20] = [
        Relation::NamedRel,
        Relation::Optional,
        Relation::Exists,
        Relation::Comprehension,
        Relation::Unroll,
        Relation::Distinct,
        Relation::Aggregate,
        Relation::Unwind,
        Relation::CollectUnwind,
        Relation::LocyRule,
        Relation::LocyFold,
        Relation::LocyReach,
        Relation::LocyNegation,
        Relation::LocyQueryWhere,
        Relation::LocyCommands,
        Relation::LocyBinding,
        Relation::LocyPermuteConjuncts,
        Relation::LocyAlphaRename,
        Relation::LocyRenameIntoCollision,
        Relation::LocyExplainAgrees,
    ];

    /// Short name for reports and failures.
    pub fn name(self) -> &'static str {
        match self {
            Relation::NamedRel => "named_rel",
            Relation::Optional => "optional",
            Relation::Exists => "exists",
            Relation::Comprehension => "comprehension",
            Relation::Unroll => "unroll",
            Relation::Distinct => "distinct",
            Relation::Aggregate => "aggregate",
            Relation::Unwind => "unwind",
            Relation::CollectUnwind => "collect_unwind",
            Relation::LocyRule => "locy_rule",
            Relation::LocyFold => "locy_fold",
            Relation::LocyReach => "locy_reach",
            Relation::LocyNegation => "locy_negation",
            Relation::LocyQueryWhere => "locy_query_where",
            Relation::LocyCommands => "locy_commands",
            Relation::LocyBinding => "locy_binding",
            Relation::LocyPermuteConjuncts => "locy_permute_conjuncts",
            Relation::LocyAlphaRename => "locy_alpha_rename",
            Relation::LocyRenameIntoCollision => "locy_rename_into_collision",
            Relation::LocyExplainAgrees => "locy_explain_agrees",
        }
    }

    /// Programs this relation expects to fail to compile for `case`, each
    /// with a word its error message must contain.
    pub fn expected_errors(self, case: &TopoCase) -> Vec<(String, &'static str)> {
        match self {
            // `m` is exposed by `p2` and by `f`: reading it is ambiguous
            // whatever the order of the references.
            Relation::LocyPermuteConjuncts => vec![(
                three_ref_reader(case)
                    .with_tail(
                        "YIELD KEY {a}, KEY {b}, {v}, {m} \
                         QUERY {q} RETURN {a}.id AS c0, {b}.id AS c1, {m} AS c2",
                    )
                    .render(&case.perm, &Names::new()),
                "ambiguous",
            )],
            _ => Vec::new(),
        }
    }

    /// Whether [`Self::transforms`] reports anything for this relation.
    fn transforms_reported(self) -> bool {
        matches!(self, Relation::LocyPermuteConjuncts)
    }

    /// Whether this relation's rewrite of `case` changes the program's text
    /// in the way the relation is about, or `None` when every rewrite does.
    ///
    /// A conjunct permutation can draw the identity, which compares a program
    /// with itself; the driver holds the share of non-identity draws to
    /// [`MIN_TRANSFORMED_RATE`].
    pub fn transforms(self, case: &TopoCase) -> Option<bool> {
        match self {
            // The three-reference reader takes the whole permutation.
            Relation::LocyPermuteConjuncts => Some(case.perm != IDENTITY),
            _ => None,
        }
    }

    /// The formulations this relation compares for `case`, or `None` when it
    /// does not apply.
    pub fn rewrite(self, case: &TopoCase) -> Option<Rewrite> {
        let ret = case.items(false);
        match self {
            Relation::NamedRel => Some(Rewrite {
                reference: format!("{} RETURN {ret}", case.joined(Naming::Anonymous)),
                equivalents: vec![vec![format!("{} RETURN {ret}", case.joined(Naming::Named))]],
            }),
            Relation::Optional => {
                let pattern = case.pattern(Naming::Anonymous);
                Some(Rewrite {
                    reference: format!(
                        "MATCH (a:Person){} OPTIONAL MATCH {pattern}{} RETURN {ret}",
                        case.anchor_where(),
                        case.inner_where()
                    ),
                    equivalents: vec![vec![
                        format!("{} RETURN {ret}", case.joined(Naming::Anonymous)),
                        format!(
                            "MATCH (a:Person){} RETURN {}",
                            case.with_anchor(&format!("NOT EXISTS {}", case.subquery(&pattern))),
                            case.items(true)
                        ),
                    ]],
                })
            }
            Relation::Exists => {
                let ret = case.a_items();
                let pattern = case.pattern(Naming::Anonymous);
                let mut equivalents = vec![vec![format!(
                    "MATCH (a:Person){} RETURN {ret}",
                    case.with_anchor(&format!("COUNT {} > 0", case.subquery(&pattern)))
                )]];
                if case.inner.is_none() {
                    equivalents.push(vec![format!(
                        "MATCH (a:Person){} RETURN {ret}",
                        case.with_anchor(&case.pattern_with(Naming::Anonymous, false, None))
                    )]);
                }
                Some(Rewrite {
                    reference: format!(
                        "MATCH (a:Person){} RETURN {ret}",
                        case.with_anchor(&format!("EXISTS {}", case.subquery(&pattern)))
                    ),
                    equivalents,
                })
            }
            Relation::Comprehension => {
                let pattern = case.pattern(Naming::Anonymous);
                Some(Rewrite {
                    reference: format!(
                        "MATCH (a:Person){} RETURN a.id AS c0, size([{pattern}{} | 1]) AS k",
                        case.anchor_where(),
                        case.inner_where()
                    ),
                    equivalents: vec![vec![format!(
                        "MATCH (a:Person){} RETURN a.id AS c0, COUNT {} AS k",
                        case.anchor_where(),
                        case.subquery(&pattern)
                    )]],
                })
            }
            Relation::Unroll => {
                let (at, (lo, hi)) = case.ranged_hop()?;
                let filter = |pattern: String| {
                    let conds: Vec<String> = case
                        .anchor
                        .iter()
                        .chain(case.inner.iter())
                        .map(Pred::render)
                        .collect();
                    let filter = if conds.is_empty() {
                        String::new()
                    } else {
                        format!(" WHERE {}", conds.join(" AND "))
                    };
                    format!("MATCH (a:Person) MATCH {pattern}{filter} RETURN {ret}")
                };
                let by_range = (lo..=hi)
                    .map(|k| {
                        filter(case.pattern_with(Naming::Anonymous, true, Some((at, Ok((k, k))))))
                    })
                    .collect();
                let by_hops = (lo..=hi)
                    .map(|k| filter(case.pattern_with(Naming::Anonymous, true, Some((at, Err(k))))))
                    .collect();
                Some(Rewrite {
                    reference: filter(case.pattern(Naming::Anonymous)),
                    equivalents: vec![by_range, by_hops],
                })
            }
            Relation::Distinct => {
                let cols: Vec<String> = (0..case.items.len()).map(|i| format!("c{i}")).collect();
                Some(Rewrite {
                    reference: format!("{} RETURN DISTINCT {ret}", case.joined(Naming::Anonymous)),
                    equivalents: vec![vec![format!(
                        "{} WITH {ret}, count(*) AS n RETURN {}",
                        case.joined(Naming::Anonymous),
                        cols.join(", ")
                    )]],
                })
            }
            Relation::Aggregate => {
                let joined = case.joined(Naming::Anonymous);
                let fold = |init: &str, step: &str| format!("reduce(m = {init}, v IN xs | {step})");
                Some(Rewrite {
                    reference: format!(
                        "{joined} RETURN a.id AS c0, count(*) AS n, count(b.age) AS k, \
                         min(b.age) AS lo, max(b.age) AS hi, sum(b.age) AS s, avg(b.age) AS m"
                    ),
                    equivalents: vec![vec![format!(
                        "{joined} WITH a, collect(b.age) AS xs, collect(1) AS all \
                         RETURN a.id AS c0, size(all) AS n, size(xs) AS k, {} AS lo, {} AS hi, \
                         {} AS s, CASE size(xs) WHEN 0 THEN null \
                         ELSE toFloat({}) / size(xs) END AS m",
                        fold("null", "CASE WHEN m IS NULL OR v < m THEN v ELSE m END"),
                        fold("null", "CASE WHEN m IS NULL OR v > m THEN v ELSE m END"),
                        // `sum` over no non-null value is 0, as the fold's start.
                        fold("0", "m + v"),
                        fold("0", "m + v"),
                    )]],
                })
            }
            Relation::Unwind => {
                // The list is the anchor, so the case's anchor predicate is left
                // out: with it, too few draws matched any row to compare.
                let tail = |anchor: &str| {
                    let conds: Vec<String> = std::iter::once(anchor.to_string())
                        .chain(case.inner.iter().map(Pred::render))
                        .collect();
                    format!(
                        "MATCH {} WHERE {}",
                        case.pattern(Naming::Anonymous),
                        conds.join(" AND ")
                    )
                };
                let list: Vec<String> = case.unwind.iter().map(i64::to_string).collect();
                Some(Rewrite {
                    reference: format!(
                        "UNWIND [{}] AS u MATCH (a:Person) {} RETURN u AS u, {ret}",
                        list.join(", "),
                        tail("a.id = u")
                    ),
                    equivalents: vec![
                        case.unwind
                            .iter()
                            .map(|v| {
                                format!(
                                    "MATCH (a:Person) {} RETURN {v} AS u, {ret}",
                                    tail(&format!("a.id = {v}"))
                                )
                            })
                            .collect(),
                    ],
                })
            }
            Relation::CollectUnwind => {
                let joined = case.joined(Naming::Anonymous);
                Some(Rewrite {
                    reference: format!(
                        "{joined} WITH collect(b.age) AS xs UNWIND xs AS x RETURN x AS c0"
                    ),
                    equivalents: vec![vec![format!(
                        "{joined} WITH b.age AS x WHERE x IS NOT NULL RETURN x AS c0"
                    )]],
                })
            }
            Relation::LocyRule => Some(Rewrite {
                reference: format!(
                    "CREATE RULE r AS MATCH {}{} YIELD KEY a, KEY b \
                     QUERY r RETURN a.id AS c0, b.id AS c1",
                    case.locy_pattern(),
                    case.body_where()
                ),
                equivalents: vec![vec![format!(
                    "{} RETURN DISTINCT a.id AS c0, b.id AS c1",
                    case.joined(Naming::Anonymous)
                )]],
            }),
            Relation::LocyFold => Some(Rewrite {
                // FOLD `SUM` is Cypher's `sum` (an integer sum stays an
                // integer, issue #316) and `MIN` / `MAX` keep the input's
                // type, so the Cypher side converts nothing.
                reference: format!(
                    "CREATE RULE f AS MATCH {}{} \
                     FOLD n = COUNT(*), s = SUM(b.age), lo = MIN(b.id), hi = MAX(b.id) \
                     YIELD KEY a, n, s, lo, hi \
                     QUERY f RETURN a.id AS c0, n AS c1, s AS c2, lo AS c3, hi AS c4",
                    case.locy_pattern(),
                    case.body_where()
                ),
                equivalents: vec![vec![format!(
                    "{} RETURN a.id AS c0, count(*) AS c1, sum(b.age) AS c2, \
                     min(b.id) AS c3, max(b.id) AS c4",
                    case.joined(Naming::Anonymous)
                )]],
            }),
            Relation::LocyReach => {
                let hop = case.hops[0];
                // A walk may revisit an edge and a trail may not, so an
                // undirected walk returns to its start where no trail does;
                // directed, a closed walk contains a cycle, which is a trail.
                let distinct = matches!(hop.dir, Dir::Both).then_some("a.id <> b.id");
                let filter = where_all(
                    case.anchor
                        .iter()
                        .map(Pred::render)
                        .chain(distinct.map(String::from)),
                );
                Some(Rewrite {
                    reference: format!(
                        "{} QUERY reach{filter} RETURN a.id AS c0, b.id AS c1",
                        reach_rules(hop)
                    ),
                    equivalents: vec![vec![format!(
                        "MATCH (a){}(b){filter} RETURN DISTINCT a.id AS c0, b.id AS c1",
                        render_rel(hop, Some((1, u32::MAX)), None)
                            .replace(&format!("..{}", u32::MAX), "..")
                    )]],
                })
            }
            Relation::LocyNegation => {
                let hop = case.hops[0];
                // Undirected walks and trails disagree at a = b (above).
                if matches!(hop.dir, Dir::Both) {
                    return None;
                }
                // Without an anchor every pair would be checked, which is slow;
                // a case without one gets a few fixed ids. Requiring the case's
                // own anchor applied this relation to about one case in eight,
                // too few for a small lane's activation check.
                let anchor = case
                    .anchor
                    .as_ref()
                    .map_or_else(|| "a.id IN [0, 1, 5, 9]".to_string(), Pred::render);
                let vlp = render_rel(hop, Some((1, u32::MAX)), None)
                    .replace(&format!("..{}", u32::MAX), "..");
                Some(Rewrite {
                    reference: format!(
                        "{} CREATE RULE un AS MATCH (a:Person), (b:Person) \
                         WHERE a IS NOT reach TO b YIELD KEY a, KEY b \
                         QUERY un WHERE {} RETURN a.id AS c0, b.id AS c1",
                        reach_rules(hop),
                        anchor
                    ),
                    equivalents: vec![vec![format!(
                        "MATCH (a:Person), (b:Person) WHERE {} \
                         AND NOT EXISTS {{ MATCH (a){vlp}(b) }} RETURN a.id AS c0, b.id AS c1",
                        anchor
                    )]],
                })
            }
            Relation::LocyQueryWhere => {
                // Move the anchor predicate, the inner one, or both, from the
                // rule body to the QUERY. A case with neither gets a few fixed
                // anchor ids, so the relation applies to every case: moving only
                // the inner one applied to about one case in eight, too few for
                // a small lane's activation check to mean anything.
                let inner = case.inner.as_ref().map(Pred::render);
                let anchor = case
                    .anchor
                    .as_ref()
                    .map(Pred::render)
                    .or_else(|| inner.is_none().then(|| "a.id IN [0, 1, 5, 9]".to_string()));
                let program = |body: &[&String], query: &[&String]| {
                    format!(
                        "CREATE RULE r AS MATCH {}{} YIELD KEY a, KEY b \
                         QUERY r{} RETURN a.id AS c0, b.id AS c1",
                        case.locy_pattern(),
                        where_all(body.iter().map(|p| (*p).clone())),
                        where_all(query.iter().map(|p| (*p).clone())),
                    )
                };
                let both: Vec<&String> = anchor.iter().chain(inner.iter()).collect();
                let mut equivalents = Vec::new();
                if let Some(i) = &inner {
                    let kept: Vec<&String> = anchor.iter().collect();
                    equivalents.push(vec![program(&kept, &[i])]);
                }
                if let Some(a) = &anchor {
                    let kept: Vec<&String> = inner.iter().collect();
                    equivalents.push(vec![program(&kept, &[a])]);
                }
                if both.len() == 2 {
                    equivalents.push(vec![program(&[], &both)]);
                }
                Some(Rewrite {
                    reference: program(&both, &[]),
                    equivalents,
                })
            }
            Relation::LocyCommands => {
                // Recursion, a FOLD, and a rule reading both (the FOLD's value
                // column by its bare name), so a command that disturbed any of
                // them shows in the one QUERY. Each EXPLAIN matches nothing:
                // with no provenance to read, EXPLAIN re-derives the rule
                // through the in-memory evaluator, which is the path that once
                // wrote its rows into the store the QUERY reads.
                let program = |command: &str| {
                    LocyText {
                        rules: format!(
                            "{} CREATE RULE f AS MATCH {}{} \
                             FOLD n = COUNT(*), s = SUM(b.age) YIELD KEY a, n, s ",
                            reach_rules(case.hops[0]),
                            case.locy_pattern(),
                            case.body_where()
                        ),
                        matches: "(a:Person), (b:Person)".to_string(),
                        conjuncts: vec!["a IS reach TO b".to_string(), "a IS f".to_string()],
                        tail: format!(
                            "YIELD KEY a, KEY b, n \
                             {command} QUERY out RETURN a.id AS c0, b.id AS c1, n AS c2"
                        ),
                    }
                    .render(&IDENTITY, &names(&[("q", "out")]))
                };
                // One command: an EXPLAIN of `out` that matches nothing
                // re-derives `out`, and through it `f` and `reach`, so it
                // reaches every rule. Each further command cost about 35 s a
                // run of this lane (measured: 82 s without the relation, 224 s
                // with four commands); ABDUCE and per-rule EXPLAIN are covered
                // by `locy_command_noninterference`.
                let equivalents = vec![vec![program("EXPLAIN RULE out WHERE a.id = -1")]];
                Some(Rewrite {
                    reference: program(""),
                    equivalents,
                })
            }
            Relation::LocyBinding => {
                // `p` counts each (a, b) pair's matches. Its value column `n`
                // is read four ways; the last reads a copy of `p` that yields
                // `n` first and its KEYs in the other order, which binds
                // `(b, a, v)` to the same columns: KEY columns bind first, in
                // YIELD order, then the value.
                let rule = |name: &str, yield_items: &str| {
                    format!(
                        "CREATE RULE {name} AS MATCH {}{} FOLD n = COUNT(*) YIELD {yield_items} ",
                        case.locy_pattern(),
                        case.body_where()
                    )
                };
                let p = rule("p", "KEY a, KEY b, n");
                let reader = |rules: &str, conjunct: &str, yield_items: &str| {
                    LocyText {
                        rules: rules.to_string(),
                        matches: "(a), (b)".to_string(),
                        conjuncts: vec![conjunct.to_string()],
                        tail: format!(
                            "YIELD {yield_items} QUERY q RETURN a.id AS c0, b.id AS c1, v AS c2"
                        ),
                    }
                    .render(&IDENTITY, &Names::new())
                };
                Some(Rewrite {
                    reference: reader(&p, "(a, b, v) IS p", "KEY a, KEY b, v"),
                    equivalents: vec![
                        vec![reader(&p, "(a, b) IS p TO v", "KEY a, KEY b, v")],
                        vec![reader(&p, "(a, b) IS p", "KEY a, KEY b, n AS v")],
                        vec![reader(
                            &rule("p", "n, KEY b, KEY a"),
                            "(b, a, v) IS p",
                            "KEY a, KEY b, v",
                        )],
                    ],
                })
            }
            Relation::LocyPermuteConjuncts => {
                let p1 = p1_reader(case);
                let three = three_ref_reader(case);
                let none = Names::new();
                Some(Rewrite {
                    reference: p1.render(&IDENTITY, &none),
                    equivalents: vec![
                        vec![p1.render(&case.perm, &none)],
                        vec![three.render(&case.perm, &none)],
                        vec![three.render_joined(&case.perm, &none, " AND ")],
                    ],
                })
            }
            Relation::LocyAlphaRename => {
                let p1 = p1_reader(case);
                Some(Rewrite {
                    reference: p1.render(&IDENTITY, &Names::new()),
                    equivalents: vec![vec![p1.render(
                        &IDENTITY,
                        &names(LOCY_RENAMINGS[case.renaming % LOCY_RENAMINGS.len()]),
                    )]],
                })
            }
            Relation::LocyRenameIntoCollision => {
                let p1 = p1_reader(case);
                Some(Rewrite {
                    reference: p1.render(&IDENTITY, &Names::new()),
                    equivalents: vec![
                        vec![p1.render(
                            &IDENTITY,
                            &names(P1_COLLISIONS[case.renaming % P1_COLLISIONS.len()]),
                        )],
                        vec![three_ref_reader(case).render(
                            &IDENTITY,
                            &names(
                                THREE_REF_COLLISIONS[case.renaming % THREE_REF_COLLISIONS.len()],
                            ),
                        )],
                    ],
                })
            }
            Relation::LocyExplainAgrees => {
                // EXPLAIN re-resolves each fact's clause, so the facts are
                // restricted to a few anchors: the case's UNWIND ids plus two
                // even ids with edges, which the seed rule keeps.
                let mut ids: Vec<i64> = case.unwind.iter().copied().chain([0, 18]).collect();
                ids.sort_unstable();
                ids.dedup();
                let ids: Vec<String> = ids.iter().map(i64::to_string).collect();
                let anchors = format!("{{a}}.id IN [{}]", ids.join(", "));
                let p1 = p1_reader(case);
                let none = Names::new();
                Some(Rewrite {
                    reference: p1
                        .with_tail(format!(
                            "YIELD KEY {{a}}, KEY {{b}}, {{v}} QUERY {{q}} WHERE {anchors} \
                             RETURN DISTINCT {{a}}.id AS c0, {{b}}.id AS c1"
                        ))
                        .render(&case.perm, &none),
                    equivalents: vec![vec![explain_part(
                        &["a", "b"],
                        &["s", "p"],
                        &p1.with_tail(format!(
                            "YIELD KEY {{a}}, KEY {{b}}, {{v}} EXPLAIN RULE {{q}} WHERE {anchors}"
                        ))
                        .render(&case.perm, &none),
                    )]],
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Minimum fraction of applicable cases whose reference query returns rows.
///
/// Measured rates on the flushed fixture are reported on every run; this floor
/// sits well below the lowest of them, so a generator or fixture drift that
/// empties the results fails loudly instead of passing on empty bags.
pub const MIN_NON_EMPTY_RATE: f64 = 0.30;

/// Applicable cases below which the rate is too noisy to hold to
/// [`MIN_NON_EMPTY_RATE`]; such a run only has to produce one non-empty case.
/// Measured: the 25-case small-batch run drew `unwind` non-empty in 5 and in 13
/// of 25 on two runs of the same code.
const MIN_CASES_FOR_RATE: u64 = 40;

/// Rows one case may return across every query of every relation.
const PER_CASE_ROW_CEILING: usize = 20_000;

fn cases_from_env(default: u32) -> u32 {
    std::env::var("DQP_CASES")
        .ok()
        .map(|v| v.parse().expect("DQP_CASES must be a positive integer"))
        .unwrap_or(default)
}

/// Minimum fraction of applicable cases whose rewrite actually transforms
/// the program, for relations whose draw can be the identity
/// ([`Relation::transforms`]). A conjunct permutation of three draws the
/// identity one time in six.
pub const MIN_TRANSFORMED_RATE: f64 = 0.50;

#[derive(Default, Clone, Copy)]
struct Tally {
    applied: u64,
    non_empty: u64,
    /// Applicable cases whose rewrite moved something, for relations that
    /// report it.
    transformed: u64,
}

async fn run_bag(db: &Uni, query: &str) -> anyhow::Result<RowBag> {
    if let Some(marked) = query.strip_prefix(EXPLAIN_MARK) {
        let (header, program) = marked
            .split_once('\n')
            .ok_or_else(|| anyhow::anyhow!("malformed explain part: {query}"))?;
        let (keys, refs) = header
            .split_once(';')
            .ok_or_else(|| anyhow::anyhow!("malformed explain header: {header}"))?;
        let keys: Vec<&str> = keys.split(',').collect();
        let refs: Vec<&str> = refs.split(',').collect();
        return run_explain_bag(db, &keys, &refs, program).await;
    }
    if query.starts_with("CREATE RULE") {
        return run_locy_bag(db, query).await;
    }
    let result = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{e}\n  query: {query}"))?;
    Ok(bag(&result))
}

/// The rows of a Locy program's first `QUERY`, as a [`RowBag`] keyed by the
/// returned column names (the Cypher side's bag sorts columns by name too).
async fn run_locy_bag(db: &Uni, program: &str) -> anyhow::Result<RowBag> {
    let result = db
        .session()
        .locy(program)
        .await
        .map_err(|e| anyhow::anyhow!("{e}\n  program: {program}"))?;
    let rows = result
        .command_results()
        .iter()
        .find_map(|c| c.as_query())
        .ok_or_else(|| anyhow::anyhow!("program has no QUERY: {program}"))?;
    let mut out = RowBag::default();
    for row in rows {
        let mut columns: Vec<&String> = row.keys().collect();
        columns.sort();
        if out.columns.is_empty() {
            out.columns = columns.iter().map(|c| (*c).clone()).collect();
        }
        let canon = crate::diff::CanonRow(columns.iter().map(|c| row[*c].clone()).collect());
        *out.counts.entry(canon).or_insert(0) += 1;
        out.total += 1;
    }
    Ok(out)
}

/// The facts an `EXPLAIN RULE` program derives, as a set of `c0, c1, …` rows
/// holding the `id` of each KEY node in `keys`.
///
/// A root child is a fact. EXPLAIN answers in one of two modes
/// (`uni-query`'s `locy_explain.rs`):
///
/// - from the provenance the fixpoint recorded, whenever it has entries
///   matching the `WHERE`: each child is a recorded derivation, carries no
///   children of its own, and its `graph_fact` starts with `[iter=`;
/// - otherwise by re-resolving each published fact's clause: a fact counts
///   as derived when its derivation has a child for every rule in `refs`,
///   and one no clause re-derives comes back as a leaf with no children.
async fn run_explain_bag(
    db: &Uni,
    keys: &[&str],
    refs: &[&str],
    program: &str,
) -> anyhow::Result<RowBag> {
    let result = db
        .session()
        .locy(program)
        .await
        .map_err(|e| anyhow::anyhow!("{e}\n  program: {program}"))?;
    let root = result
        .command_results()
        .iter()
        .find_map(|c| c.as_explain())
        .ok_or_else(|| anyhow::anyhow!("program has no EXPLAIN RULE: {program}"))?;
    let mut out = RowBag {
        columns: (0..keys.len()).map(|i| format!("c{i}")).collect(),
        ..RowBag::default()
    };
    for fact in &root.children {
        let recorded = fact.children.is_empty()
            && fact
                .graph_fact
                .as_deref()
                .is_some_and(|g| g.starts_with("[iter="));
        let derived = recorded
            || refs
                .iter()
                .all(|r| fact.children.iter().any(|child| child.rule == *r));
        if !derived {
            continue;
        }
        let row = keys
            .iter()
            .map(|k| match fact.bindings.get(*k) {
                Some(uni_db::Value::Node(node)) => node
                    .properties
                    .get("id")
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("KEY {k} node has no id: {node:?}")),
                other => Err(anyhow::anyhow!(
                    "KEY {k} is not a node in EXPLAIN: {other:?}"
                )),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        out.counts.entry(crate::diff::CanonRow(row)).or_insert(1);
    }
    out.total = out.counts.len();
    Ok(out)
}

/// Runs `query` once, or returns its bag from `memo` when this case already
/// ran it.
async fn run_memo(
    db: &Uni,
    query: &str,
    memo: &mut HashMap<String, RowBag>,
) -> anyhow::Result<RowBag> {
    if let Some(b) = memo.get(query) {
        return Ok(b.clone());
    }
    let b = run_bag(db, query).await?;
    memo.insert(query.to_string(), b.clone());
    Ok(b)
}

async fn check_case(
    db: &Uni,
    case: &TopoCase,
    tallies: &RefCell<Vec<Tally>>,
) -> Result<(), String> {
    let mut rows = 0usize;
    // Several Locy relations share a reference program; each runs (twice)
    // once per case.
    let mut memo: HashMap<String, RowBag> = HashMap::new();
    for (slot, relation) in Relation::ALL.iter().enumerate() {
        let Some(rewrite) = relation.rewrite(case) else {
            continue;
        };
        let seen = memo.contains_key(&rewrite.reference);
        let reference = run_memo(db, &rewrite.reference, &mut memo)
            .await
            .map_err(|e| format!("[{}] {e}", relation.name()))?;
        rows += reference.total;
        // The same query twice must agree: an order-dependent operator shows up
        // here as itself, instead of as a flaky disagreement between rewrites.
        if !seen {
            let again = run_bag(db, &rewrite.reference)
                .await
                .map_err(|e| format!("[{}] {e}", relation.name()))?;
            if let Err(diff) = bag_eq(&reference, &again) {
                return Err(format!(
                    "[{}] the same query returned different rows on a second run.\n  query: {}\n{diff}",
                    relation.name(),
                    rewrite.reference
                ));
            }
        }
        {
            let mut t = tallies.borrow_mut();
            t[slot].applied += 1;
            if reference.total > 0 {
                t[slot].non_empty += 1;
            }
            if relation.transforms(case) == Some(true) {
                t[slot].transformed += 1;
            }
        }
        for (program, needle) in relation.expected_errors(case) {
            match db.session().locy(&program).await {
                Ok(_) => {
                    return Err(format!(
                        "[{}] expected a compile error containing `{needle}`, \
                         but the program ran.\n  program: {program}",
                        relation.name()
                    ));
                }
                Err(e) if format!("{e:#}").to_lowercase().contains(needle) => {}
                Err(e) => {
                    return Err(format!(
                        "[{}] expected an error containing `{needle}`, got: {e:#}\n  program: {program}",
                        relation.name()
                    ));
                }
            }
        }
        for parts in &rewrite.equivalents {
            let mut bags = Vec::with_capacity(parts.len());
            for part in parts {
                let b = run_memo(db, part, &mut memo)
                    .await
                    .map_err(|e| format!("[{}] {e}", relation.name()))?;
                rows += b.total;
                bags.push(b);
            }
            let equivalent = bag_union(&bags);
            if let Err(diff) = bag_eq(&reference, &equivalent) {
                return Err(format!(
                    "[{}] formulations disagree.\n  reference: {}\n  equivalent: {}\n{diff}",
                    relation.name(),
                    rewrite.reference,
                    parts.join("\n     ⊎ ")
                ));
            }
        }
        if rows > PER_CASE_ROW_CEILING {
            return Err(format!(
                "one case returned {rows} rows against a ceiling of {PER_CASE_ROW_CEILING}; \
                 reference query: {}",
                rewrite.reference
            ));
        }
    }
    Ok(())
}

/// How a topology run draws its cases.
#[derive(Clone, Copy, Debug)]
pub enum Seeding {
    /// The same cases every run. A PR lane's activation checks ("each relation
    /// returned rows in at least one case") then pass or fail on the code, not
    /// on the draw: at eight random cases and fourteen relations a few percent
    /// of runs failed by chance.
    Fixed,
    /// Fresh cases every run, for breadth: the nightly soaks.
    Random,
}

/// Runs every relation over `cases` generated cases against one fixture.
///
/// # Panics
///
/// Panics on a disagreement (after shrinking), on a query error, or when a
/// relation's non-empty rate falls below [`MIN_NON_EMPTY_RATE`].
pub fn drive_topo(
    label: &str,
    layout: Layout,
    execution_batch_size: Option<usize>,
    cases: u32,
    seeding: Seeding,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let db = rt
        .block_on(build_topo(layout, execution_batch_size))
        .expect("build topology fixture");
    let tallies = RefCell::new(vec![Tally::default(); Relation::ALL.len()]);
    let config = Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    };
    let mut runner = match seeding {
        Seeding::Fixed => {
            TestRunner::new_with_rng(config, TestRng::deterministic_rng(RngAlgorithm::ChaCha))
        }
        Seeding::Random => TestRunner::new(config),
    };
    let outcome = runner.run(&arb_topo_case(), |case| {
        rt.block_on(check_case(&db, &case, &tallies))
            .map_err(TestCaseError::fail)
    });

    let tallies = tallies.into_inner();
    let mut report = format!("[dqp:topo:{label}]");
    for (relation, t) in Relation::ALL.iter().zip(&tallies) {
        let _ = write!(report, " {}={}/{}", relation.name(), t.non_empty, t.applied);
        if relation.transforms_reported() {
            let _ = write!(report, "(moved {})", t.transformed);
        }
    }
    eprintln!("{report}");

    outcome.expect("a query-rewrite relation found a divergence");
    for (relation, t) in Relation::ALL.iter().zip(&tallies) {
        assert!(
            t.applied > 0,
            "[{label}] relation {} never applied",
            relation.name()
        );
        assert!(
            t.non_empty > 0,
            "[{label}] relation {}: none of {} applicable cases returned rows",
            relation.name(),
            t.applied
        );
        if relation.transforms_reported() {
            assert!(
                t.transformed > 0,
                "[{label}] relation {}: none of {} applicable cases transformed the program",
                relation.name(),
                t.applied
            );
        }
        if t.applied < MIN_CASES_FOR_RATE {
            continue;
        }
        if relation.transforms_reported() {
            let moved = t.transformed as f64 / t.applied as f64;
            assert!(
                moved >= MIN_TRANSFORMED_RATE,
                "[{label}] relation {}: only {:.1}% of {} applicable cases transformed the \
                 program (floor {:.0}%); comparing a program with itself proves nothing",
                relation.name(),
                moved * 100.0,
                t.applied,
                MIN_TRANSFORMED_RATE * 100.0
            );
        }
        let rate = t.non_empty as f64 / t.applied as f64;
        assert!(
            rate >= MIN_NON_EMPTY_RATE,
            "[{label}] relation {}: only {:.1}% of {} applicable cases returned rows \
             (floor {:.0}%); comparisons over empty bags prove nothing",
            relation.name(),
            rate * 100.0,
            t.applied,
            MIN_NON_EMPTY_RATE * 100.0
        );
    }
}

/// All rows flushed, default execution batch size.
///
/// 40 cases: with the Locy relations a case costs about 1.8 s (107 s for 60
/// alone), and the full suite runs it beside everything else under this
/// test's six-minute nextest limit (`.config/nextest.toml`).
#[test]
fn topo_rewrites_flushed() {
    drive_topo(
        "flushed",
        Layout::Flushed,
        None,
        cases_from_env(40),
        Seeding::Fixed,
    );
}

/// Half the graph in L0 and a two-row execution batch, so every multi-row
/// intermediate spans batches.
///
/// Fewer cases than the flushed run: a two-row batch makes every operator pay
/// its per-batch cost per pair of rows, measured at about 6 s per case with
/// the Locy relations (93 s for 15), so 8 cases take about 50 s.
#[test]
fn topo_rewrites_unflushed_small_batches() {
    drive_topo(
        "unflushed-b2",
        Layout::HalfUnflushed,
        Some(2),
        cases_from_env(8),
        Seeding::Fixed,
    );
}

/// Nightly volume over the flushed fixture: a tenth of `DQP_CASES`. At about
/// 1.8 s a case the nightly 10 000 becomes 1 000 cases, about 30 minutes —
/// inside the soak profile's 54-minute cap.
#[test]
#[ignore = "soak: run nightly under the soak profile"]
fn topo_soak() {
    drive_topo(
        "flushed-soak",
        Layout::Flushed,
        None,
        (cases_from_env(20_000) / 10).max(1),
        Seeding::Random,
    );
}

/// Nightly volume over L0 and two-row batches: a fortieth of `DQP_CASES`,
/// since each case costs about 6 s here; the nightly 10 000 becomes 250 cases,
/// about 25 minutes.
#[test]
#[ignore = "soak: run nightly under the soak profile"]
fn topo_small_batches_soak() {
    drive_topo(
        "unflushed-b2-soak",
        Layout::HalfUnflushed,
        Some(2),
        (cases_from_env(20_000) / 40).max(1),
        Seeding::Random,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fixture has the shapes the module claims.
    #[tokio::test]
    async fn topo_fixture_has_its_shapes() -> anyhow::Result<()> {
        let db = build_topo(Layout::Flushed, None).await?;
        let count = |q: &'static str| {
            let db = &db;
            async move {
                let r = db.session().query(q).await?;
                anyhow::Ok(r.rows()[0].values()[0].clone())
            }
        };
        use uni_db::Value;
        assert_eq!(
            count("MATCH (:Person {id: 9})-[r:KNOWS]->(:Person {id: 10}) RETURN count(r)").await?,
            Value::Int(3)
        );
        assert_eq!(
            count("MATCH (n {id: 12})-[r:KNOWS]->(n) RETURN count(r)").await?,
            Value::Int(2)
        );
        assert_eq!(
            count("MATCH p = (:Person {id: 5})-[:KNOWS*2..2]->(:Person {id: 8}) RETURN count(p)")
                .await?,
            Value::Int(2)
        );
        assert_eq!(
            count("MATCH (:Person {id: 13})-[:KNOWS*3..3]->(n:Person {id: 13}) RETURN count(n)")
                .await?,
            Value::Int(1)
        );
        assert_eq!(
            count(
                "MATCH (n:Person) WHERE n.id >= 30 AND n.id < 36 AND NOT (n)--() RETURN count(n)"
            )
            .await?,
            Value::Int(6)
        );
        assert_eq!(
            count("MATCH ()-[r:KNOWS]->() RETURN count(r)").await?,
            Value::Int(edges().len() as i64)
        );
        // Every Person's integer `id` is its own VID, and `age` values 10,
        // 20, 30 are VIDs of other Persons: a VID-vs-integer confusion (an
        // integer column joined against `_vid`) binds a real node and returns
        // rows instead of hiding behind an empty result. The seed rule's `k`
        // (`a.id`) relies on it.
        assert_eq!(
            count("MATCH (n:Person) WHERE id(n) = n.id RETURN count(n)").await?,
            Value::Int(PERSONS.end - PERSONS.start)
        );
        assert_eq!(
            count("MATCH (p:Person), (n:Person) WHERE id(n) = p.age RETURN count(DISTINCT p.age)")
                .await?,
            Value::Int(3)
        );
        Ok(())
    }

    /// The half-unflushed layout has rows on both sides of the flush.
    #[tokio::test]
    async fn topo_fixture_half_unflushed_spans_l0() -> anyhow::Result<()> {
        let db = build_topo(Layout::HalfUnflushed, None).await?;
        let r = db
            .session()
            .query("MATCH ()-[r:KNOWS]->() RETURN count(r)")
            .await?;
        assert_eq!(
            r.rows()[0].values()[0],
            uni_db::Value::Int((edges().len() + late_edges().len()) as i64)
        );
        assert!(r.metrics().l0_reads > 0, "no row was read from L0");
        assert!(
            r.metrics().storage_reads > 0,
            "no row was read from storage"
        );
        Ok(())
    }

    /// A relation that compares a query with a *different* one must fail: the
    /// driver's comparison is not vacuous.
    #[tokio::test]
    async fn topo_disagreement_is_reported() -> anyhow::Result<()> {
        let db = build_topo(Layout::Flushed, None).await?;
        let a = run_bag(&db, "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.id AS c0").await?;
        let b = run_bag(
            &db,
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN DISTINCT a.id AS c0",
        )
        .await?;
        assert!(
            bag_eq(&a, &b).is_err(),
            "parallel edges must make these differ"
        );
        Ok(())
    }
}
