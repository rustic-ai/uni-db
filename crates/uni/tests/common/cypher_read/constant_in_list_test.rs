//! `x IN <constant list>` — a literal or parameter list — answered by a probe
//! of a per-batch hash index rather than a scan of the list per row.
//!
//! Two properties are pinned:
//!
//! - **Semantics.** For every element and list in a value matrix, the constant
//!   list answers exactly what the same list bound through `WITH` answers. The
//!   `WITH`-bound list reaches `_cypher_in` as a column, not a scalar, so it
//!   takes the per-row scan and serves as the oracle. That the two arms really
//!   take different paths is shown by the revert check recorded in the Black
//!   Book: breaking the index probe turns the constant arm red and leaves the
//!   oracle arm unchanged.
//! - **Cost.** Growing rows and list together by 4x grows a scan 16x and a
//!   probe 4x. The guard compares that growth against a linear control query
//!   over the same rows and the same parameter, so machine speed cancels out.

use std::time::{Duration, Instant};

use uni_db::{Uni, Value};

const BIG: i64 = 1 << 53;

const NODE_IN_IDS: &str = "MATCH (n:P) RETURN n.k AS i, n IN $l AS r ORDER BY i";

fn map(entries: &[(&str, Value)]) -> Value {
    Value::Map(
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
    )
}

/// Elements and list items: numerics around the points where widening an
/// integer to a float stops being exact (2^53) and where equality stops being
/// reflexive (NaN), strings, booleans, nested
/// lists and maps with and without nulls, and temporals.
fn values() -> Vec<Value> {
    vec![
        Value::Null,
        Value::Int(0),
        Value::Int(1),
        Value::Int(2),
        Value::Int(-1),
        Value::Int(BIG),
        Value::Int(BIG + 1),
        Value::Float(0.0),
        Value::Float(-0.0),
        Value::Float(1.0),
        Value::Float(2.5),
        Value::Float(BIG as f64),
        Value::Float(f64::NAN),
        Value::Bool(true),
        Value::Bool(false),
        Value::String(String::new()),
        Value::String("1".into()),
        Value::String("a".into()),
        Value::List(vec![]),
        Value::List(vec![Value::Int(1)]),
        Value::List(vec![Value::Float(1.0)]),
        Value::List(vec![Value::Int(1), Value::Null]),
        Value::List(vec![Value::Null]),
        map(&[("k", Value::Int(1))]),
        map(&[("k", Value::Float(1.0))]),
        map(&[("k", Value::Null)]),
        Value::Temporal(uni_common::TemporalValue::Date {
            days_since_epoch: 1,
        }),
        Value::Temporal(uni_common::TemporalValue::Duration {
            months: 1,
            days: 0,
            nanos: 0,
        }),
    ]
}

fn lists() -> Vec<Vec<Value>> {
    let all = values();
    let mut lists: Vec<Vec<Value>> = all.iter().map(|v| vec![v.clone()]).collect();
    lists.push(vec![]);
    lists.push(all.clone());
    lists.push(all.iter().filter(|v| !v.is_null()).cloned().collect());
    lists.push(vec![Value::Int(1), Value::Null, Value::String("1".into())]);
    lists.push(vec![Value::Int(BIG + 1), Value::Int(BIG + 2)]);
    lists.push((0..40).map(Value::Int).collect());
    lists
}

/// A Cypher literal for `v`, or `None` where the value has no literal syntax.
fn literal(v: &Value) -> Option<String> {
    Some(match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) if f.is_finite() => format!("{f:?}"),
        Value::String(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'")),
        Value::List(items) => format!(
            "[{}]",
            items
                .iter()
                .map(literal)
                .collect::<Option<Vec<_>>>()?
                .join(", ")
        ),
        Value::Map(m) => format!(
            "{{{}}}",
            m.iter()
                .map(|(k, v)| literal(v).map(|l| format!("{k}: {l}")))
                .collect::<Option<Vec<_>>>()?
                .join(", ")
        ),
        _ => return None,
    })
}

fn cases() -> Value {
    Value::List(
        values()
            .into_iter()
            .enumerate()
            .map(|(i, v)| map(&[("i", Value::Int(i as i64)), ("v", v)]))
            .collect(),
    )
}

/// Run `cypher` (columns `i`, `r`) and return `r` per row, ordered by `i`.
async fn answers(db: &Uni, cypher: &str, list: &[Value]) -> Vec<Value> {
    let result = db
        .session()
        .query_with(cypher)
        .param("cases", cases())
        .param("l", Value::List(list.to_vec()))
        .fetch_all()
        .await
        .unwrap_or_else(|e| panic!("{cypher} with $l = {list:?}: {e}"));
    result
        .rows()
        .iter()
        .map(|row| row.value("r").cloned().unwrap_or(Value::Null))
        .collect()
}

/// Constant list (parameter and literal) against the `WITH`-bound oracle, for
/// every element and list in the matrix.
#[tokio::test]
async fn constant_in_list_matches_with_bound_list() {
    let db = Uni::in_memory().build().await.unwrap();
    let param = "UNWIND $cases AS c RETURN c.i AS i, c.v IN $l AS r ORDER BY i";
    let oracle = "UNWIND $cases AS c WITH c, $l AS l RETURN c.i AS i, c.v IN l AS r ORDER BY i";
    let elements = values();
    let (mut trues, mut nulls, mut literal_lists) = (0, 0, 0);
    for list in lists() {
        let want = answers(&db, oracle, &list).await;
        assert_eq!(want.len(), elements.len());
        trues += want.iter().filter(|r| **r == Value::Bool(true)).count();
        nulls += want.iter().filter(|r| r.is_null()).count();

        let got = answers(&db, param, &list).await;
        for (i, (g, w)) in got.iter().zip(&want).enumerate() {
            assert_eq!(
                g, w,
                "{:?} IN $l with $l = {list:?}: parameter list said {g:?}, WITH-bound {w:?}",
                elements[i]
            );
        }

        if let Some(text) = literal(&Value::List(list.clone())) {
            literal_lists += 1;
            let q = format!("UNWIND $cases AS c RETURN c.i AS i, c.v IN {text} AS r ORDER BY i");
            let got = answers(&db, &q, &list).await;
            for (i, (g, w)) in got.iter().zip(&want).enumerate() {
                assert_eq!(
                    g, w,
                    "{:?} IN {text}: literal list said {g:?}, WITH-bound {w:?}",
                    elements[i]
                );
            }
        }
    }
    // Discrimination floor: matches and nulls occur in bulk, and most lists
    // were also probed as literals.
    assert!(trues > 50 && nulls > 50, "trues={trues} nulls={nulls}");
    assert!(literal_lists > 20, "only {literal_lists} literal lists");
}

/// Anchor answers on the constant path, so agreement with the oracle is not
/// agreement on a shared mistake.
#[tokio::test]
async fn constant_in_list_known_answers() {
    let db = Uni::in_memory().build().await.unwrap();
    let cases = [
        ("1 IN [1.0]", Value::Bool(true)),
        ("1.0 IN [1]", Value::Bool(true)),
        ("2 IN [1, null]", Value::Null),
        ("null IN []", Value::Bool(false)),
        ("null IN [1]", Value::Null),
        ("'1' IN [1]", Value::Bool(false)),
        ("[1] IN [[1.0]]", Value::Bool(true)),
        ("[1] IN [[null]]", Value::Null),
        ("{k: 1} IN [{k: null}]", Value::Null),
        ("0.0/0.0 IN [0.0/0.0]", Value::Bool(false)),
        ("9007199254740993 IN [9007199254740992]", Value::Bool(false)),
        // Numbers compare exactly: `2^53 + 1` is not the float `2^53`.
        (
            "9007199254740993 IN [9007199254740992.0]",
            Value::Bool(false),
        ),
        (
            "9007199254740992 IN [9007199254740992.0]",
            Value::Bool(true),
        ),
        (
            "date('2020-01-01') IN [date('2020-01-01')]",
            Value::Bool(true),
        ),
    ];
    for (expr, want) in cases {
        // Several rows, so the batch carries the element as a column.
        let q = format!("UNWIND range(1, 3) AS x RETURN {expr} AS r");
        let result = db.session().query(&q).await.unwrap();
        for row in result.rows() {
            assert_eq!(row.value("r"), Some(&want), "{expr}");
        }
    }
}

/// Nodes against a constant id list (the left side is rewritten to its id) and
/// a property against a constant value list, each against the `WITH` oracle.
#[tokio::test]
async fn constant_in_list_entities_and_properties() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("UNWIND range(0, 19) AS k CREATE (:P {k: k, f: toFloat(k) / 2})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let vids: Vec<Value> = db
        .session()
        .query("MATCH (n:P) WHERE n.k % 3 = 0 RETURN id(n) AS v")
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|r| r.value("v").cloned().unwrap())
        .collect();
    let mut ids = vids.clone();
    ids.push(Value::Null);
    ids.push(Value::Float(1.5));

    let pairs = [
        (
            NODE_IN_IDS,
            "WITH $l AS l MATCH (n:P) RETURN n.k AS i, n IN l AS r ORDER BY i",
            ids.clone(),
        ),
        (
            NODE_IN_IDS,
            "WITH $l AS l MATCH (n:P) RETURN n.k AS i, n IN l AS r ORDER BY i",
            vids.clone(),
        ),
        (
            "MATCH (n:P) RETURN n.k AS i, n.f IN $l AS r ORDER BY i",
            "WITH $l AS l MATCH (n:P) RETURN n.k AS i, n.f IN l AS r ORDER BY i",
            vec![Value::Int(1), Value::Float(2.5), Value::String("3".into())],
        ),
        (
            "MATCH (n:P) RETURN n.k AS i, n.k IN $l AS r ORDER BY i",
            "WITH $l AS l MATCH (n:P) RETURN n.k AS i, n.k IN l AS r ORDER BY i",
            vec![Value::Float(4.0), Value::Int(7), Value::Null],
        ),
    ];
    let entity_values = (
        // `ns[j]` is a whole entity value, not rewritten to its id, so this
        // is the arm where an entity element meets a list of bare ids.
        "MATCH (n:P) WITH collect(n) AS ns UNWIND range(0, size(ns) - 1) AS j \
         RETURN ns[j].k AS i, ns[j] IN $l AS r ORDER BY i",
        "MATCH (n:P) WITH collect(n) AS ns, $l AS l UNWIND range(0, size(ns) - 1) AS j \
         RETURN ns[j].k AS i, ns[j] IN l AS r ORDER BY i",
        // Integers only: a parameter mixing integers with a float is widened
        // to a float list, and an entity equals only an integer id.
        vids.iter()
            .cloned()
            .chain([Value::Null])
            .collect::<Vec<_>>(),
    );
    for (constant, oracle, list) in pairs.into_iter().chain([entity_values.clone()]) {
        let got = answers(&db, constant, &list).await;
        let want = answers(&db, oracle, &list).await;
        assert_eq!(got.len(), 20, "{constant}");
        assert_eq!(got, want, "{constant} vs {oracle} with $l = {list:?}");
    }
    // Anchor: exactly the multiples of 3 are in the id list, whether the
    // node is rewritten to its id or arrives as a whole entity value.
    for q in [NODE_IN_IDS, entity_values.0] {
        let got = answers(&db, q, &vids).await;
        let trues = got.iter().filter(|r| **r == Value::Bool(true)).count();
        assert_eq!(trues, 7, "{q}: {got:?}");
    }
}

/// Best of three wall times for `cypher` over `rows` rows and an `ids` list of
/// `len` misses.
async fn best_time(db: &Uni, cypher: &str, rows: i64, len: i64) -> Duration {
    let ids = Value::List((0..len).map(|i| Value::Int(-1 - i)).collect());
    let mut best = Duration::MAX;
    for _ in 0..3 {
        let start = Instant::now();
        let result = db
            .session()
            .query_with(cypher)
            .param("n", Value::Int(rows))
            .param("ids", ids.clone())
            .fetch_all()
            .await
            .unwrap();
        best = best.min(start.elapsed());
        assert_eq!(result.rows().len(), 1);
    }
    best
}

/// A constant `IN` list scales like a linear query, not like rows × list.
///
/// Rows and list grow together 4x. The per-row scan then grows 16x; the probe,
/// like the control (a filter that reads the same parameter but compares
/// nothing per item), grows about 4x. The bound — `IN` growth under 2.5x the
/// control's growth — sits between 1x (probe) and about 4x (scan), so it holds
/// with a wide margin either way. Wall time rather than a counter because the
/// comparison count is a property of `_cypher_in`, which its own unit test
/// (`constant_list_cost_does_not_grow_with_list`) pins exactly; this test pins
/// that a real query reaches that path. Best of three absorbs scheduler noise.
#[tokio::test]
async fn constant_in_list_cost_is_linear() {
    let db = Uni::in_memory().build().await.unwrap();
    let probe = "UNWIND range(1, $n) AS x WITH x WHERE x IN $ids RETURN count(*) AS c";
    let control = "UNWIND range(1, $n) AS x WITH x WHERE x <> size($ids) RETURN count(*) AS c";
    let (small, large) = (1_500, 6_000);

    // Warm-up, so first-query setup lands in neither measurement.
    best_time(&db, probe, 10, 10).await;

    let probe_growth = best_time(&db, probe, large, large).await.as_secs_f64()
        / best_time(&db, probe, small, small).await.as_secs_f64();
    let control_growth = best_time(&db, control, large, large).await.as_secs_f64()
        / best_time(&db, control, small, small).await.as_secs_f64();
    assert!(
        probe_growth < 2.5 * control_growth,
        "IN grew {probe_growth:.1}x for 4x rows and list; linear control grew {control_growth:.1}x"
    );
}

/// A list parameter reaches the query with its items intact.
///
/// A list with no common native element type used to have each item
/// stringified into bytes, so `$l = [[]]` arrived as `[Bytes("[]")]` and
/// `[] IN $l` was false where `[] IN [[]]` is true. The differential above
/// cannot see this — both its arms read the same parameter — so the parameter
/// is checked against itself and against literal answers here.
#[tokio::test]
async fn list_parameter_items_survive() {
    let db = Uni::in_memory().build().await.unwrap();
    let date = Value::Temporal(uni_common::TemporalValue::Date {
        days_since_epoch: 18_262,
    });
    let lists = [
        Value::List(vec![Value::List(vec![])]),
        Value::List(vec![
            Value::List(vec![Value::Int(1)]),
            Value::List(vec![Value::Int(1), Value::Null]),
        ]),
        Value::List(vec![Value::Int(1), Value::String("a".into())]),
        Value::List(vec![Value::Bool(true), Value::Int(1), Value::Null]),
        Value::List(vec![date.clone(), date.clone()]),
        Value::List(vec![map(&[("k", Value::Int(1))])]),
        Value::List(vec![Value::Null, Value::List(vec![Value::Int(1)])]),
    ];
    for list in &lists {
        let result = db
            .session()
            .query_with("UNWIND range(1, 2) AS x RETURN $l AS r")
            .param("l", list.clone())
            .fetch_all()
            .await
            .unwrap();
        for row in result.rows() {
            assert_eq!(row.value("r"), Some(list), "RETURN $l");
        }
    }

    let probes = [
        (
            "[] IN $l",
            Value::List(vec![Value::List(vec![])]),
            Value::Bool(true),
        ),
        ("[1] IN $l", lists[1].clone(), Value::Bool(true)),
        ("[2] IN $l", lists[1].clone(), Value::Bool(false)),
        ("[1, 2] IN $l", lists[1].clone(), Value::Null),
        ("'a' IN $l", lists[2].clone(), Value::Bool(true)),
        ("'1' IN $l", lists[2].clone(), Value::Bool(false)),
        (
            "date('2020-01-01') IN $l",
            lists[4].clone(),
            Value::Bool(true),
        ),
        (
            "date('2020-01-02') IN $l",
            lists[4].clone(),
            Value::Bool(false),
        ),
        ("{k: 1.0} IN $l", lists[5].clone(), Value::Bool(true)),
    ];
    for (expr, list, want) in probes {
        let q = format!("UNWIND range(1, 3) AS x RETURN {expr} AS r");
        let result = db
            .session()
            .query_with(&q)
            .param("l", list.clone())
            .fetch_all()
            .await
            .unwrap();
        for row in result.rows() {
            assert_eq!(row.value("r"), Some(&want), "{expr} with $l = {list:?}");
        }
    }
}

/// A CypherValue compares with a list literal of any element type.
///
/// The literal side is converted to a CypherValue first, and that converter
/// rejected element types outside a short list — including the `Null` type of
/// `[]` and `Date32` — so `c.v = []` failed with "Unsupported element type for
/// typed_large_list_to_cv_array".
#[tokio::test]
async fn cypher_value_equals_untyped_list_literal() {
    let db = Uni::in_memory().build().await.unwrap();
    let cases = [
        ("c.v = []", Value::List(vec![]), Value::Bool(true)),
        (
            "c.v = []",
            Value::List(vec![Value::Int(1)]),
            Value::Bool(false),
        ),
        (
            "c.v = [date('2020-01-01')]",
            Value::List(vec![Value::Temporal(uni_common::TemporalValue::Date {
                days_since_epoch: 18_262,
            })]),
            Value::Bool(true),
        ),
    ];
    for (expr, v, want) in cases {
        let q = format!("UNWIND $cases AS c RETURN {expr} AS r");
        let result = db
            .session()
            .query_with(&q)
            .param(
                "cases",
                Value::List(vec![map(&[("i", Value::Int(0)), ("v", v.clone())])]),
            )
            .fetch_all()
            .await
            .unwrap_or_else(|e| panic!("{expr} with c.v = {v:?}: {e}"));
        assert_eq!(
            result.rows()[0].value("r"),
            Some(&want),
            "{expr} with c.v = {v:?}"
        );
    }
}
