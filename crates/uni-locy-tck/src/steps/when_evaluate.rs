use crate::LocyWorld;
use cucumber::when;
use uni_locy::LocyConfig;

/// True when a `QUERY ... WHERE` clause cannot remove any row.
///
/// The corpus idiom is `QUERY r WHERE n = n RETURN ...`, a self-comparison used
/// to bind the goal variable rather than to filter. It appears in **56 of the
/// 99** `QUERY ... WHERE` clauses in the feature files; treating every `WHERE`
/// as potentially-filtering would drop the parity check from 71 of 114 queries
/// to 15, which is the difference between a guard with teeth and a decorative
/// one.
///
/// Deliberately conservative: only `x = x` on a bare variable, and `AND` of
/// such terms, count as tautologies. Anything else — a property comparison, a
/// parameter, a function call — is treated as a real filter and skipped.
fn where_clause_cannot_filter(expr: &uni_cypher::ast::Expr) -> bool {
    use uni_cypher::ast::{BinaryOp, Expr};
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOp::Eq => {
                matches!((left.as_ref(), right.as_ref()),
                    (Expr::Variable(a), Expr::Variable(b)) if a == b)
            }
            BinaryOp::And => where_clause_cannot_filter(left) && where_clause_cannot_filter(right),
            _ => false,
        },
        _ => false,
    }
}

/// A comparable value, normalized so the two engines can be compared at all.
///
/// `Value`'s own `Hash`/`Eq` cover every property of a `Node`, which makes
/// whole-node comparison unusable across engines: they legitimately populate
/// different property sets for the same node. Identity is the only stable part.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
enum IdentKey {
    Vid(u64),
    Eid(u64),
    Scalar(String),
    /// A value deliberately excluded from comparison — see
    /// [`normalize_for_compare`].
    Opaque,
}

/// Normalizes a value for cross-engine comparison.
///
/// Floats collapse to [`IdentKey::Opaque`]: the fixpoint and SLG paths perform
/// probability arithmetic in different orders and differ in the last bits, so
/// including them would produce false failures rather than real findings.
fn normalize_for_compare(value: &uni_common::Value) -> IdentKey {
    use uni_common::Value;
    match value {
        Value::Node(n) => IdentKey::Vid(n.vid.as_u64()),
        Value::Edge(e) => IdentKey::Eid(e.eid.as_u64()),
        Value::Float(_) => IdentKey::Opaque,
        other => IdentKey::Scalar(format!("{other:?}")),
    }
}

/// Maps each rule name to its KEY column names, by re-parsing the program.
///
/// Uses `resolve_yield_column_names` — the same function the planner and the SLG
/// resolver use — so the names are authoritative rather than a second guess at
/// the naming convention. A rule may have several clauses; the first with a
/// `YIELD` wins, and they must agree on the schema anyway.
fn key_columns_by_rule(
    ast: &uni_cypher::locy_ast::LocyProgram,
) -> std::collections::HashMap<String, Vec<String>> {
    use uni_cypher::locy_ast::{resolve_yield_column_names, LocyStatement, RuleOutput};

    let mut out: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    for stmt in &ast.statements {
        let LocyStatement::Rule(def) = stmt else {
            continue;
        };
        let RuleOutput::Yield(yield_clause) = &def.output else {
            continue;
        };
        let name = def.name.to_string();
        if out.contains_key(&name) {
            continue;
        }
        let names = resolve_yield_column_names(&yield_clause.items);
        let keys: Vec<String> = names
            .iter()
            .zip(&yield_clause.items)
            .filter(|(_, item)| item.is_key)
            .map(|(n, _)| n.clone())
            .collect();
        if !keys.is_empty() {
            out.insert(name, keys);
        }
    }
    out
}

/// One comparable key column, and how to read it from each surface.
///
/// A `RETURN a.name` projects a *property* of the key, so the two surfaces hold
/// different things for it: `derived` carries the whole node under `a`, while
/// the query row carries the string under `a.name`. Comparing them directly
/// would compare a vid against a string and fail on every scenario — the same
/// property has to be extracted from the derived node as well.
#[derive(Debug, Clone)]
struct KeyProjection {
    /// Column holding the key in `derived` rows.
    key_col: String,
    /// Property to read off it, when the RETURN projected one.
    prop: Option<String>,
    /// Column holding the value in the QUERY rows.
    out_name: String,
}

/// Reads a key projection from a `derived` row.
fn derived_side(row: &uni_locy::FactRow, p: &KeyProjection) -> IdentKey {
    use uni_common::Value;
    let Some(base) = row.get(&p.key_col) else {
        return IdentKey::Opaque;
    };
    let Some(prop) = &p.prop else {
        return normalize_for_compare(base);
    };
    match base {
        Value::Node(n) => n
            .properties
            .get(prop)
            .map_or(IdentKey::Opaque, normalize_for_compare),
        Value::Edge(e) => e
            .properties
            .get(prop)
            .map_or(IdentKey::Opaque, normalize_for_compare),
        Value::Map(m) => m.get(prop).map_or(IdentKey::Opaque, normalize_for_compare),
        // A scalar key column with a property access is not comparable.
        _ => IdentKey::Opaque,
    }
}

/// Which output column each key column is visible under in a `QUERY`'s rows.
///
/// With no `RETURN`, rows carry the rule's yield names directly. With a
/// `RETURN`, a key is only comparable when some item projects it as a bare
/// variable (`RETURN p`) or a property of it (`RETURN a.name`); the output name
/// follows the alias, else the OpenCypher default. Keys that no item projects
/// are simply not comparable and are dropped.
///
/// Returns `None` when the RETURN contains an aggregate, which can collapse
/// rows and makes any cardinality comparison meaningless.
fn comparable_key_projection(
    keys: &[String],
    return_clause: Option<&uni_cypher::ast::ReturnClause>,
) -> Option<Vec<KeyProjection>> {
    use uni_cypher::ast::{Expr, ReturnItem};

    let bare = |k: &String| KeyProjection {
        key_col: k.clone(),
        prop: None,
        out_name: k.clone(),
    };
    let Some(rc) = return_clause else {
        return Some(keys.iter().map(bare).collect());
    };

    let mut out = Vec::new();
    for item in &rc.items {
        // `RETURN *` keeps the underlying names.
        let ReturnItem::Expr { expr, alias, .. } = item else {
            out.extend(keys.iter().map(bare));
            continue;
        };
        if expr_contains_aggregate(expr) {
            return None;
        }
        let (key_col, prop, default_name) = match expr {
            Expr::Variable(v) if keys.contains(v) => (v.clone(), None, v.clone()),
            Expr::Property(inner, prop) => match inner.as_ref() {
                Expr::Variable(v) if keys.contains(v) => {
                    (v.clone(), Some(prop.clone()), format!("{v}.{prop}"))
                }
                _ => continue,
            },
            _ => continue,
        };
        out.push(KeyProjection {
            key_col,
            prop,
            out_name: alias.clone().unwrap_or(default_name),
        });
    }
    Some(out)
}

/// Conservative aggregate detection — any known aggregate function name.
fn expr_contains_aggregate(expr: &uni_cypher::ast::Expr) -> bool {
    use uni_cypher::ast::Expr;
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            const AGGREGATES: [&str; 7] = ["count", "sum", "avg", "min", "max", "collect", "stdev"];
            AGGREGATES.contains(&name.to_lowercase().as_str())
                || args.iter().any(expr_contains_aggregate)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => expr_contains_aggregate(expr),
        _ => false,
    }
}

/// Asserts that `QUERY <rule>` answers with the facts in `derived[<rule>]` —
/// checked on **every** scenario that evaluates a program.
///
/// `QUERY` was once served by a separate top-down SLG resolver that re-derived
/// from scratch, and the two engines do not have equal expressive power, so a
/// rule could derive facts on one surface and return nothing on the other
/// (<https://github.com/rustic-ai/uni-db/issues/160>). Both now read the facts
/// the fixpoint published, but they are still two reads of two stores (the
/// fixpoint's and the hydrated row store commands read), so this asserts they
/// agree rather than assuming it.
///
/// `SemanticParity.feature` already tried to catch this class with seven
/// hand-written scenarios, and #160 slipped through the gap between them. This
/// is the same idea moved to the funnel every evaluation passes through, so
/// coverage is a property of the corpus rather than of what someone remembered
/// to write.
///
/// **Non-vacuity, not equality.** Strict row-count equality is not assertable
/// generically because `QUERY` carries `WHERE` / `RETURN` / `LIMIT`. But no
/// projection, `DISTINCT`, `ORDER BY` or aggregate can turn a non-empty
/// relation into zero rows — only a `WHERE` or a `LIMIT` can. So for a `QUERY`
/// with neither, non-empty `derived` implies non-empty rows. Every other shape
/// is skipped rather than guessed at.
async fn assert_query_derived_parity(program: &str, result: &uni_locy::LocyResult) {
    use uni_cypher::locy_ast::LocyStatement;

    // A program the TCK feeds us that does not re-parse is not this guard's
    // problem — the scenario's own assertions cover it.
    let Ok(ast) = uni_cypher::parse_locy(program) else {
        return;
    };

    let goal_queries: Vec<_> = ast
        .statements
        .iter()
        .filter_map(|s| match s {
            LocyStatement::GoalQuery(gq) => Some(gq),
            _ => None,
        })
        .collect();

    // Commands run in sequence, and `derived` holds the facts before the
    // first command. A QUERY after a command that writes (a DERIVE, a Cypher
    // write) reads facts derived again in the state that write left, which
    // `derived` does not describe; only the QUERYs ahead of the first write
    // read the same state, so only they are compared. The QUERYs after it
    // are covered by `locy_toplevel_sequencing`'s oracle.
    let comparable = ast
        .statements
        .iter()
        .take_while(|s| match s {
            LocyStatement::DeriveCommand(_) => false,
            LocyStatement::Cypher(q) => !uni_locy::incremental::query_may_write(q),
            _ => true,
        })
        .filter(|s| matches!(s, LocyStatement::GoalQuery(_)))
        .count();

    let query_rows: Vec<_> = result
        .command_results
        .iter()
        .filter_map(|cr| match cr {
            uni_locy::CommandResult::Query(rows) => Some(rows),
            _ => None,
        })
        .collect();

    // Both sequences follow program order, so index i is the same QUERY on both
    // sides. If the counts disagree the alignment is unknown — skip rather than
    // risk blaming the wrong rule.
    if goal_queries.len() != query_rows.len() {
        return;
    }

    let key_columns = key_columns_by_rule(&ast);

    for (gq, rows) in goal_queries.iter().zip(query_rows).take(comparable) {
        // A real WHERE, a LIMIT or a SKIP can legitimately empty or shrink the
        // result; a self-comparison tautology cannot.
        if gq
            .where_expr
            .as_ref()
            .is_some_and(|w| !where_clause_cannot_filter(w))
        {
            continue;
        }
        if gq
            .return_clause
            .as_ref()
            .is_some_and(|r| r.limit.is_some() || r.skip.is_some())
        {
            continue;
        }

        let rule = gq.rule_name.to_string();
        let Some(derived) = result.derived_rules().await.unwrap().get(&rule) else {
            continue;
        };
        if derived.is_empty() {
            continue;
        }

        // Strong check: the QUERY's key tuples are exactly the ones the
        // fixpoint derived. Both read the published facts, so a missing tuple
        // is a lost answer and an extra one is a fact the fixpoint never
        // derived (a command once wrote re-derived rows into the store QUERY
        // reads; see `locy_command_noninterference`).
        let projection = key_columns
            .get(&rule)
            .and_then(|keys| comparable_key_projection(keys, gq.return_clause.as_ref()))
            .filter(|p| !p.is_empty());

        if let Some(projection) = projection {
            let derived_keys: std::collections::HashSet<Vec<IdentKey>> = derived
                .iter()
                .map(|row| projection.iter().map(|p| derived_side(row, p)).collect())
                .collect();
            let query_keys: std::collections::HashSet<Vec<IdentKey>> = rows
                .iter()
                .map(|row| {
                    projection
                        .iter()
                        .map(|p| {
                            row.get(&p.out_name)
                                .map(normalize_for_compare)
                                .unwrap_or(IdentKey::Opaque)
                        })
                        .collect()
                })
                .collect();

            let missing: Vec<_> = derived_keys.difference(&query_keys).collect();
            let extra: Vec<_> = query_keys.difference(&derived_keys).collect();
            assert!(
                extra.is_empty(),
                "QUERY/derived parity violated for rule `{rule}`: the QUERY result \
                 has {} key tuple(s) the fixpoint did not derive, and the query has \
                 no WHERE / LIMIT / SKIP that could explain it.\n\
                 Extra: {extra:?}\n\
                 Program:\n{program}",
                extra.len()
            );
            assert!(
                missing.is_empty(),
                "QUERY/derived parity violated for rule `{rule}`: {} of {} derived \
                 key tuple(s) are absent from the QUERY result ({} rows), and the \
                 query has no WHERE / LIMIT / SKIP that could explain it.\n\
                 Missing: {missing:?}\n\
                 This is the issue #160 class: `derived` and `QUERY` disagree.\n\
                 Program:\n{program}",
                missing.len(),
                derived_keys.len(),
                rows.len()
            );
            continue;
        }

        // Fallback when no key column is comparable (RETURN projects only
        // expressions or aggregates): the original non-vacuity invariant.
        if rows.is_empty() {
            panic!(
                "QUERY/derived parity violated for rule `{rule}`: the fixpoint \
                 derived {} fact(s) but QUERY returned 0 rows, and the query has \
                 no WHERE / LIMIT / SKIP that could explain it.\n\
                 This is the issue #160 class: `derived` and `QUERY` disagree.\n\
                 Program:\n{program}",
                derived.len()
            );
        }
    }
}

/// If the Locy evaluation produced a `DerivedFactSet`, apply it to the database
/// via a transaction so that DERIVE mutations are visible to subsequent `then` steps.
///
/// Session-level DERIVE uses `collect_derive: true`, which defers mutations into a
/// `DerivedFactSet` instead of auto-applying. The TCK expects mutations to be
/// visible immediately, so we apply them here and update `stats.mutations_executed`
/// to reflect the actual mutations performed.
async fn apply_derived_and_store(
    world: &mut LocyWorld,
    program: &str,
    result: Result<uni_db::locy::LocyResult, uni_common::UniError>,
) {
    store_result(world, program, result, true).await;
}

/// Store the evaluation result without applying derived facts.
/// Used by scenarios that test DERIVE isolation (e.g. "edges do not persist without tx.apply").
async fn store_without_apply(
    world: &mut LocyWorld,
    program: &str,
    result: Result<uni_db::locy::LocyResult, uni_common::UniError>,
) {
    store_result(world, program, result, false).await;
}

async fn store_result(
    world: &mut LocyWorld,
    program: &str,
    result: Result<uni_db::locy::LocyResult, uni_common::UniError>,
    apply_derived: bool,
) {
    let result = match result {
        Ok(locy_result) => {
            let mut inner = locy_result.into_inner();
            if apply_derived {
                if let Some(derived) = inner.derived_fact_set.clone() {
                    let session = world.db().session();
                    let tx = session
                        .tx()
                        .await
                        .expect("Failed to start transaction for DERIVE apply");
                    let apply_result = tx
                        .apply(derived)
                        .await
                        .expect("Failed to apply derived facts");
                    tx.commit().await.expect("Failed to commit derived facts");
                    inner.stats.mutations_executed += apply_result.facts_applied;
                }
            }
            // Checked on every scenario, not just the parity feature. An `Err`
            // result is skipped — a failed evaluation has no parity to violate.
            assert_query_derived_parity(program, &inner).await;
            Ok(inner)
        }
        Err(e) => Err(e),
    };
    world.set_locy_result(result);
}

#[when("evaluating the following Locy program:")]
async fn when_evaluating_locy_program(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(base_config())
        .run()
        .await;
    apply_derived_and_store(world, program, result).await;
}

#[when("evaluating the following Locy program without applying derived facts:")]
async fn when_evaluating_locy_program_without_apply(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(base_config())
        .run()
        .await;
    store_without_apply(world, program, result).await;
}

/// The configuration every scenario evaluates under.
///
/// Unset, the corpus runs under the default goal-directed mode
/// (`GoalDirected::Auto`): each rewritten `QUERY` is answered by the
/// rewritten program and a rule only such goals needed is derived when a
/// step reads it, so every assertion applies unchanged.
/// `UNI_LOCY_TCK_GOAL_DIRECTED` selects another mode (`uni_locy::magic`):
/// `off` evaluates every rule in full; `shadow` keeps every rule's full
/// relation in the evaluation itself; `on` drops rules only rewritten goals
/// needed, so scenarios asserting such a rule's derived relation are expected
/// to fail in that mode.
fn base_config() -> LocyConfig {
    let goal_directed = match std::env::var("UNI_LOCY_TCK_GOAL_DIRECTED").as_deref() {
        Ok("shadow") => uni_locy::GoalDirected::Shadow,
        Ok("on") => uni_locy::GoalDirected::On,
        Ok("off") => uni_locy::GoalDirected::Off,
        Ok("auto") | Err(_) => uni_locy::GoalDirected::Auto,
        Ok(other) => {
            panic!("UNI_LOCY_TCK_GOAL_DIRECTED={other}: expected auto, off, on or shadow")
        }
    };
    LocyConfig {
        goal_directed,
        ..LocyConfig::default()
    }
}

/// Initialize the DB, build a `LocyConfig` via `mutate`, run the program in
/// the docstring, and apply derived facts. Used by every `#[when]` handler
/// that needs to set one or more config fields.
async fn run_with_config(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
    mutate: impl FnOnce(&mut LocyConfig),
) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let mut config = base_config();
    mutate(&mut config);
    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await;
    apply_derived_and_store(world, program, result).await;
}

#[when(regex = r#"^evaluating the following Locy program with max_iterations (\d+):$"#)]
async fn when_evaluating_with_max_iterations(
    world: &mut LocyWorld,
    max_iter: usize,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| c.max_iterations = max_iter).await;
}

#[when(
    regex = r#"^evaluating the following Locy program with max_iterations (\d+) allowing partial:$"#
)]
async fn when_evaluating_with_max_iterations_allow_partial(
    world: &mut LocyWorld,
    max_iter: usize,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| {
        c.max_iterations = max_iter;
        c.allow_partial = true;
    })
    .await;
}

#[when("evaluating the following Locy program with strict_probability_domain:")]
async fn when_evaluating_with_strict_probability(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| c.strict_probability_domain = true).await;
}

#[when("evaluating the following Locy program with exact_probability:")]
async fn when_evaluating_with_exact_probability(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| c.exact_probability = true).await;
}

#[when(
    regex = r#"^evaluating the following Locy program with exact_probability and max_bdd_variables (\d+):$"#
)]
async fn when_evaluating_with_exact_probability_and_bdd_limit(
    world: &mut LocyWorld,
    max_bdd: usize,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| {
        c.exact_probability = true;
        c.max_bdd_variables = max_bdd;
    })
    .await;
}

#[when(
    regex = r#"^evaluating the following Locy program with exact_probability and top_k_proofs (\d+):$"#
)]
async fn when_evaluating_with_exact_probability_and_top_k(
    world: &mut LocyWorld,
    top_k: usize,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| {
        c.exact_probability = true;
        c.top_k_proofs = top_k;
    })
    .await;
}

#[when(
    regex = r#"^evaluating the following Locy program with semiring "(AddMultProb|MaxMinProb|BddExact|TopKProofs\(\d+\))":$"#
)]
async fn when_evaluating_with_semiring(
    world: &mut LocyWorld,
    kind: String,
    step: &cucumber::gherkin::Step,
) {
    let semiring = match kind.as_str() {
        "AddMultProb" => uni_locy::SemiringKind::AddMultProb,
        "MaxMinProb" => uni_locy::SemiringKind::MaxMinProb,
        "BddExact" => uni_locy::SemiringKind::BddExact,
        s if s.starts_with("TopKProofs(") && s.ends_with(')') => {
            let k_str = &s["TopKProofs(".len()..s.len() - 1];
            let k: u32 = k_str
                .parse()
                .unwrap_or_else(|_| panic!("invalid k in semiring '{s}'"));
            uni_locy::SemiringKind::TopKProofs { k }
        }
        other => panic!("unknown semiring kind: {other}"),
    };

    run_with_config(world, step, |c| c.semiring = semiring).await;
}

/// Phase C gate-closure: the neural-predicates feature is GA;
/// `neural_predicates_preview` defaults to `true`. This step
/// explicitly sets it to `false` so scenarios can assert the
/// opt-out behavior (CREATE MODEL rejection).
#[when("evaluating the following Locy program with neural_predicates_preview disabled:")]
async fn when_evaluating_with_neural_preview_disabled(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    let classifier_registry = world.classifier_registry.clone();
    run_with_config(world, step, |c| {
        c.neural_predicates_preview = false;
        c.classifier_registry = classifier_registry;
    })
    .await;
}

#[when("evaluating the following Locy program with neural_predicates_preview:")]
async fn when_evaluating_with_neural_preview(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    // Pull any classifiers staged by `Given a registered mock classifier ...`
    // so neural invocations dispatch correctly.
    let classifier_registry = world.classifier_registry.clone();
    run_with_config(world, step, |c| {
        c.neural_predicates_preview = true;
        c.classifier_registry = classifier_registry;
    })
    .await;
}

#[when("evaluating the following Locy program with params:")]
async fn when_evaluating_with_params(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    let params = world.params().clone();
    run_with_config(world, step, |c| c.params = params).await;
}
