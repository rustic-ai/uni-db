// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end test for the `LocyGenerator` plugin surface (deferred P2 item D3):
//! a fixed-arity, non-recursive *generator* predicate that binds new variables
//! (1:N), the Datomic/Souffle-functor use case. Registers a third-party
//! `myplugin.range(n) -> (i)` generator and uses it inside a Locy rule body,
//! proving the emitted tuples explode the rule's rows and bind the output var.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow_array::{Array, ArrayRef, Int64Array};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use uni_db::{DataType, Uni};
use uni_plugin::traits::locy::{GenSignature, GeneratorOutput, LocyGenerator};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, QName, Scope, SideEffects,
};

/// `myplugin.range(n) -> (i)` — for input `n`, emits one row per `i` in `0..n`.
#[derive(Debug)]
struct RangeGen {
    sig: GenSignature,
}

impl RangeGen {
    fn new() -> Self {
        Self {
            sig: GenSignature {
                args: vec![ArgType::Primitive(arrow_schema::DataType::Int64)],
                outputs: vec![ArgType::Primitive(arrow_schema::DataType::Int64)],
                volatility: Volatility::Immutable,
            },
        }
    }
}

impl LocyGenerator for RangeGen {
    fn signature(&self) -> &GenSignature {
        &self.sig
    }

    fn generate(&self, args: &[ColumnarValue], rows: usize) -> Result<GeneratorOutput, FnError> {
        let ColumnarValue::Array(a) = &args[0] else {
            return Err(FnError::new(0, "range expects an array arg"));
        };
        let n_arr = a
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| FnError::new(0, "range expects an Int64"))?;

        let mut row_map: Vec<u32> = Vec::new();
        let mut vals: Vec<i64> = Vec::new();
        for r in 0..rows {
            if n_arr.is_null(r) {
                continue;
            }
            for i in 0..n_arr.value(r).max(0) {
                row_map.push(r as u32);
                vals.push(i);
            }
        }
        let col: ArrayRef = Arc::new(Int64Array::from(vals));
        Ok(GeneratorOutput {
            row_map,
            columns: vec![col],
        })
    }
}

struct GeneratorPlugin {
    manifest: OnceLock<PluginManifest>,
}

impl Plugin for GeneratorPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: uni_plugin::PluginId::new("myplugin"),
            version: "0.1.0".parse().expect("static version"),
            abi: AbiRange::parse("^1").expect("static abi"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::LocyGenerator]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "third-party Locy generator predicate (test)".to_owned(),
            metadata: BTreeMap::new(),
        })
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.locy_generator(
            QName::new("myplugin", "range"),
            RangeGen::new().sig,
            Arc::new(RangeGen::new()),
        )?;
        Ok(())
    }
}

/// A registered Locy generator explodes a rule's rows and binds its output var.
#[tokio::test]
async fn locy_generator_binds_and_explodes_in_rule_body() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.add_plugin(GeneratorPlugin {
        manifest: OnceLock::new(),
    })?;

    db.schema()
        .label("Cnt")
        .property("k", DataType::Int64)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // Two source rows: k=3 (emits 0,1,2) and k=2 (emits 0,1).
    tx.execute("CREATE (:Cnt {k: 3})").await?;
    tx.execute("CREATE (:Cnt {k: 2})").await?;
    tx.commit().await?;

    let program = "CREATE RULE idx AS \
         MATCH (n:Cnt) WHERE myplugin.range(n.k) -> (i) YIELD KEY n.k AS k, KEY i AS i\n\
         QUERY idx RETURN k, i";
    let result = session.locy(program).await?;
    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);

    let mut pairs: Vec<(i64, i64)> = rows
        .iter()
        .filter_map(|r| {
            Some((
                r.get("k").and_then(uni_db::Value::as_i64)?,
                r.get("i").and_then(uni_db::Value::as_i64)?,
            ))
        })
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![(2, 0), (2, 1), (3, 0), (3, 1), (3, 2)],
        "range(k) should emit i in 0..k, exploding each source row"
    );
    Ok(())
}

/// A `QUERY ... WHERE key = literal` over a generator rule keeps the rows
/// Locy's `=` keeps. The goal binding compared the key by structural `==`, so
/// `k = 3.0` against an integer `k` matched nothing and the query silently
/// returned no rows (W6).
#[tokio::test]
async fn locy_generator_goal_binding_uses_value_equality() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.add_plugin(GeneratorPlugin {
        manifest: OnceLock::new(),
    })?;
    db.schema()
        .label("Cnt")
        .property("k", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Cnt {k: 3}), (:Cnt {k: 2})").await?;
    tx.commit().await?;

    for filter in ["k = 3", "k = 3.0", "3.0 = k"] {
        let program = format!(
            "CREATE RULE idx AS \
             MATCH (n:Cnt) WHERE myplugin.range(n.k) -> (i) YIELD KEY n.k AS k, KEY i AS i\n\
             QUERY idx WHERE {filter} RETURN k, i"
        );
        let result = session.locy(&program).await?;
        let empty = vec![];
        let mut is: Vec<i64> = result
            .rows()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|r| r.get("i").and_then(uni_db::Value::as_i64))
            .collect();
        is.sort();
        assert_eq!(is, vec![0, 1, 2], "{filter}");
    }
    Ok(())
}

/// What a rule with a generator, and a rule reading it, return: the rows of
/// the last QUERY as sorted `column=value` cells.
async fn generator_rows(program: &str) -> anyhow::Result<Vec<String>> {
    let db = Uni::in_memory().build().await?;
    db.add_plugin(GeneratorPlugin {
        manifest: OnceLock::new(),
    })?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Cnt {k: 3, name: 'three'}), (:Cnt {k: 2, name: 'two'})")
        .await?;
    tx.execute(
        "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), \
         (a)-[:E {w: 1}]->(b), (b)-[:E {w: 2}]->(c)",
    )
    .await?;
    tx.commit().await?;
    let result = session.locy(program).await?;
    let empty = vec![];
    let mut rows: Vec<String> = result
        .rows()
        .unwrap_or(&empty)
        .iter()
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v}")).collect();
            cells.sort();
            cells.join(" ")
        })
        .collect();
    rows.sort();
    Ok(rows)
}

const IDX: &str =
    "CREATE RULE idx AS MATCH (n:Cnt) WHERE myplugin.range(n.k) -> (i) YIELD KEY n, KEY i AS i\n";

/// A generator rule is derived by the fixpoint like any other rule.
///
/// The fixpoint used to skip every rule with a generator, and only a QUERY
/// re-derived it through the in-memory evaluator, so a rule reading it saw no
/// facts (`IS` matched nothing, `IS NOT` matched everything) and its own FOLD,
/// BEST BY and post-FOLD WHERE were ignored, without an error.
#[tokio::test]
async fn locy_generator_rule_is_derived_by_the_fixpoint() -> anyhow::Result<()> {
    let cases: Vec<(&str, String, &[&str])> = vec![
        (
            "IS against a generator rule",
            format!("{IDX}CREATE RULE has AS MATCH (n:Cnt) WHERE n IS idx YIELD KEY n\nQUERY has RETURN n.name AS n"),
            &["n=three", "n=two"],
        ),
        (
            "IS NOT against a generator rule",
            format!("{IDX}CREATE RULE no AS MATCH (n:Cnt) WHERE n IS NOT idx YIELD KEY n\nQUERY no RETURN n.name AS n"),
            &[],
        ),
        (
            "FOLD over a generator rule's facts",
            format!("{IDX}CREATE RULE cnt AS MATCH (n:Cnt) WHERE n IS idx TO i FOLD c = COUNT(*) YIELD KEY n, c\nQUERY cnt RETURN n.name AS n, c"),
            &["c=2 n=two", "c=3 n=three"],
        ),
        (
            "FOLD in a generator rule",
            "CREATE RULE g AS MATCH (n:Cnt) WHERE myplugin.range(n.k) -> (i) FOLD s = SUM(i) YIELD KEY n, s\nQUERY g RETURN n.name AS n, s".to_string(),
            &["n=three s=3", "n=two s=1"],
        ),
        (
            "post-FOLD WHERE in a generator rule",
            "CREATE RULE g AS MATCH (n:Cnt) WHERE myplugin.range(n.k) -> (i) FOLD s = SUM(i) WHERE s > 2 YIELD KEY n, s\nQUERY g RETURN n.name AS n, s".to_string(),
            &["n=three s=3"],
        ),
        (
            "BEST BY in a generator rule",
            "CREATE RULE g AS MATCH (n:Cnt) WHERE myplugin.range(n.k) -> (i) BEST BY i DESC YIELD KEY n, i AS i\nQUERY g RETURN n.name AS n, i".to_string(),
            &["i=1 n=two", "i=2 n=three"],
        ),
        (
            "a generator over another rule's value column",
            "CREATE RULE out AS MATCH (x:N)-[e:E]->(y:N) YIELD KEY x, e.w AS w\nCREATE RULE g AS MATCH (x:N) WHERE x IS out, myplugin.range(w) -> (i) YIELD KEY x, KEY i AS i\nQUERY g RETURN x.name AS x, i".to_string(),
            &["i=0 x=a", "i=0 x=b", "i=1 x=b"],
        ),
        (
            "a WHERE over a generator output",
            "CREATE RULE g AS MATCH (n:Cnt) WHERE myplugin.range(n.k) -> (i), i > 0 YIELD KEY n, KEY i AS i\nQUERY g RETURN n.name AS n, i".to_string(),
            &["i=1 n=three", "i=1 n=two", "i=2 n=three"],
        ),
    ];
    let mut failures = Vec::new();
    for (label, program, want) in &cases {
        match generator_rows(program).await {
            Ok(got) if got == *want => {}
            Ok(got) => failures.push(format!("{label}: expected {want:?}, got {got:?}")),
            Err(e) => failures.push(format!("{label}: failed: {e:#}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// `prepare_locy` runs a generator rule as `locy` does.
///
/// The generator used to be found through a registry only `Session::locy`
/// installed, so a prepared program failed with "generator is not
/// registered"; the operator now resolves it from the planner's own registry.
#[tokio::test]
async fn locy_generator_runs_prepared() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.add_plugin(GeneratorPlugin {
        manifest: OnceLock::new(),
    })?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Cnt {k: 3}), (:Cnt {k: 2})").await?;
    tx.commit().await?;
    let program = format!("{IDX}QUERY idx RETURN n.k AS k, i");
    let prepared = session.prepare_locy(&program).await?;
    let result = prepared.execute(&[]).await?;
    let empty = vec![];
    let mut pairs: Vec<(i64, i64)> = result
        .rows()
        .unwrap_or(&empty)
        .iter()
        .filter_map(|r| {
            Some((
                r.get("k").and_then(uni_db::Value::as_i64)?,
                r.get("i").and_then(uni_db::Value::as_i64)?,
            ))
        })
        .collect();
    pairs.sort();
    assert_eq!(pairs, vec![(2, 0), (2, 1), (3, 0), (3, 1), (3, 2)]);
    Ok(())
}

/// A rule with a generator condition runs `LocyGenerateExec` in its body.
#[tokio::test]
async fn a_generator_rule_runs_the_generate_operator() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.add_plugin(GeneratorPlugin {
        manifest: OnceLock::new(),
    })?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Cnt {k: 2})").await?;
    tx.commit().await?;
    crate::plan_shape::assert_locy_plan_uses(
        &session,
        &format!("{IDX}QUERY idx RETURN i"),
        "LocyGenerateExec",
    )
    .await;
    Ok(())
}
