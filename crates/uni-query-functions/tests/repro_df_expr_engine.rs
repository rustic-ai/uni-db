// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runnable repros for verified findings in the translation / type-coercion /
//! aggregate layers. Finding [1] inspects the real `apply_type_coercion`
//! output; findings [6] and [8] drive the translated expression / aggregate
//! through a real DataFusion `SessionContext` and observe the wrong value.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, LargeBinaryArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::{Expr as DfExpr, col, lit};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use uni_common::Value;
use uni_common::cypher_value_codec::encode_int;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr as CyExpr};

use uni_query_functions::df_expr::{apply_type_coercion, cypher_expr_to_df};
use uni_query_functions::df_udfs::{create_cypher_sum_udaf, register_cypher_udfs};

/// Finding [1] df_expr.rs:3368 — `coerce_case_expr` wraps the WHEN of a
/// simple/operand CASE in `_cv_to_bool` (whose type is Null); the subsequent
/// `rewrite_simple_case_to_generic` then compares the operand against a
/// Null-typed WHEN, hitting NullInvolved and emitting a literal-NULL condition,
/// so the branch is unreachable and the CASE always yields the ELSE value.
#[test]
fn repro_finding_01_simple_case_when_becomes_literal_null() {
    // Both operand and WHEN are LargeBinary (schemaless CypherValue) columns.
    let arrow_schema = Schema::new(vec![
        Field::new("op", DataType::LargeBinary, true),
        Field::new("w", DataType::LargeBinary, true),
    ]);
    let df_schema = DFSchema::try_from(arrow_schema).unwrap();

    // CASE op WHEN w THEN 1 ELSE 0 END  (simple/operand form: expr = Some(op)).
    let case = DfExpr::Case(Case {
        expr: Some(Box::new(col("op"))),
        when_then_expr: vec![(Box::new(col("w")), Box::new(lit(1_i64)))],
        else_expr: Some(Box::new(lit(0_i64))),
    });

    let coerced = apply_type_coercion(&case, &df_schema).unwrap();

    let DfExpr::Case(generic) = coerced else {
        panic!("expected a CASE, got {coerced:?}");
    };
    // The simple CASE was rewritten to a generic CASE (operand folded away).
    assert!(generic.expr.is_none(), "operand should be folded into WHEN");

    let when_condition = &*generic.when_then_expr[0].0;
    // FIXED (df_expr.rs): a simple-CASE WHEN value is no longer wrapped in
    // `_cv_to_bool`, so rewrite_simple_case_to_generic builds a real operand==value
    // comparison instead of a literal-NULL condition — the `THEN` branch is
    // reachable again when op == w.
    assert!(
        !matches!(
            when_condition,
            DfExpr::Literal(ScalarValue::Boolean(None), _)
        ),
        "WHEN condition must be a real comparison, not a literal NULL: {when_condition:?}"
    );
}

/// Finding [6] df_expr.rs:1374 — the regex operator `=~` was translated as
/// `regexp_match(left, right).is_not_null()`, collapsing NULL inputs to `false`
/// instead of propagating NULL per Cypher three-valued semantics.
///
/// `=~` now translates to a `_cypher_regex` placeholder that the planner binds
/// in two passes: type coercion turns a string-typed pair into a native regex
/// match, and anything else (a CypherValue operand) resolves to the registered
/// per-row UDF. The repro therefore drives the same pipeline the planner runs
/// (translate, resolve, coerce, resolve) rather than evaluating the bare
/// translation, and checks both bindings, since each one answers NULL on its
/// own code path.
#[tokio::test]
async fn repro_finding_06_regex_null_collapses_to_false() {
    // A Utf8 column binds the native match; a schemaless CypherValue column
    // binds the `_cypher_regex` UDF.
    let utf8_null: Arc<dyn Array> = Arc::new(StringArray::from(vec![None as Option<&str>]));
    let cv_null: Arc<dyn Array> = Arc::new(LargeBinaryArray::from(vec![None as Option<&[u8]>]));
    for (label, data_type, column) in [
        ("native string match", DataType::Utf8, utf8_null),
        ("CypherValue UDF", DataType::LargeBinary, cv_null),
    ] {
        let result = eval_regex_against_foo(data_type, column).await;
        // FIXED (df_expr.rs): `null =~ 'foo'` propagates NULL per Cypher
        // three-valued logic instead of collapsing to a concrete false.
        assert!(
            result.is_null(0),
            "[{label}] null =~ pattern must yield NULL, not a concrete boolean: {result:?}"
        );
    }
}

/// Evaluates Cypher `x =~ 'foo'` over a single-row column `x`, binding the
/// translated expression the way the physical planner does.
async fn eval_regex_against_foo(data_type: DataType, column: Arc<dyn Array>) -> BooleanArray {
    use datafusion::common::tree_node::{Transformed, TreeNode};

    let cy = CyExpr::BinaryOp {
        left: Box::new(CyExpr::Variable("x".to_string())),
        op: BinaryOp::Regex,
        right: Box::new(CyExpr::Literal(CypherLiteral::String("foo".to_string()))),
    };
    let translated = cypher_expr_to_df(&cy, None).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("x", data_type, true)]));
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
    let batch = RecordBatch::try_new(schema, vec![column]).unwrap();

    let ctx = SessionContext::new();
    register_cypher_udfs(&ctx).unwrap();
    let state = ctx.state();
    // Mirrors `HybridPhysicalPlanner::resolve_udfs`: swap each placeholder for
    // the registered UDF of the same name.
    let resolve = |e: DfExpr| {
        e.transform_up(|e| match &e {
            DfExpr::ScalarFunction(f) => match state.scalar_functions().get(f.func.name()) {
                Some(udf) => Ok(Transformed::yes(DfExpr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction {
                        func: udf.clone(),
                        args: f.args.clone(),
                    },
                ))),
                None => Ok(Transformed::no(e)),
            },
            _ => Ok(Transformed::no(e)),
        })
        .unwrap()
        .data
    };
    let bound = resolve(apply_type_coercion(&resolve(translated), &df_schema).unwrap());

    let out = ctx
        .read_batch(batch)
        .unwrap()
        .select(vec![bound.alias("r")])
        .unwrap()
        .collect()
        .await
        .unwrap();
    out[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("`=~` must evaluate to a Boolean column")
        .clone()
}

/// Finding [8] df_udfs.rs:6959 — `CypherSumAccumulator` accumulates integers
/// with `wrapping_add`, so SUM over integers silently wraps on i64 overflow and
/// returns a garbage integer instead of the overflow error this codebase raises
/// everywhere else for integer arithmetic.
#[tokio::test]
async fn repro_finding_08_sum_integer_wraps_on_overflow() {
    let ctx = SessionContext::new();
    ctx.register_udaf(create_cypher_sum_udaf());

    // Two integers whose sum overflows i64: i64::MAX + 1.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::LargeBinary,
        false,
    )]));
    let values = LargeBinaryArray::from_iter_values([encode_int(i64::MAX), encode_int(1)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
    ctx.register_batch("t", batch).unwrap();

    let out = ctx
        .sql("SELECT _cypher_sum(v) AS s FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let column = out[0].column(0);
    let lb = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let decoded = uni_common::cypher_value_codec::decode(lb.value(0)).unwrap();

    // FIXED (df_udfs.rs): on i64 overflow the accumulator drops the exact-int
    // path (checked_add -> all_ints=false) and returns the f64 sum, rather than
    // silently WRAPPING to i64::MIN. The result is a large positive Float, never
    // the wrapped negative integer.
    match decoded {
        Value::Float(f) => assert!(
            f > 9.0e18,
            "overflowing SUM must be a large positive float (~i64::MAX+1), got {f}"
        ),
        other => panic!("expected a Float on overflow, got {other:?} (must not wrap to i64::MIN)"),
    }
}
