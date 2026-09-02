//! A list literal whose items are *columns* of differing types.
//!
//! Cypher lists are heterogeneous by definition; Arrow's `make_array` requires a
//! single child type. When every item is a literal, `translate_list_literal`
//! constant-folds the mixed case into a CypherValue blob and the problem never
//! arises. When the items are property accesses, they are opaque to that
//! syntactic check — `TranslationContext` carries no field types — so the list
//! lowered to `make_array` regardless, and the mismatch surfaced only downstream.
//!
//! Both shapes below were observed failing before the fix, and they failed
//! *differently*, which is the point of testing both:
//!
//! - typed + **undeclared** property produced a plan-time error,
//!   `make_array: [Utf8, LargeBinary, Utf8]`;
//! - typed + typed (`Utf8` + `Int64`) planned successfully and then aborted the
//!   process on an Arrow assertion in `MutableArrayData`.
//!
//! The second is strictly worse and is the one LDBC SNB IC1 hits, so a fix
//! conditioned on "mixed *and* one side is `LargeBinary`" would not be enough.

use uni_db::{DataType, Uni, Value};

async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Uni")
        .property("name", DataType::String)
        .property("classYear", DataType::Int64)
        .done()
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:Uni {name: 'Caltech', classYear: 2011, undeclared: 'x'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

fn list_items(v: &Value) -> Vec<Value> {
    match v {
        Value::List(items) => items.clone(),
        other => panic!("expected a list, got {other:?}"),
    }
}

/// Two *declared* properties of different types. This is the LDBC IC1 shape.
#[tokio::test]
async fn mixed_typed_properties_in_list_literal() {
    let db = fixture().await;
    let result = db
        .session()
        .query("MATCH (u:Uni) RETURN [u.name, u.classYear] AS tuple")
        .await
        .unwrap();

    let items = list_items(result.rows()[0].value("tuple").unwrap());
    assert_eq!(items.len(), 2, "got {items:?}");
    assert_eq!(items[0], Value::String("Caltech".to_string()));
    assert_eq!(items[1], Value::Int(2011));
}

/// A declared property beside an *undeclared* one, which the scan surfaces as an
/// opaque CypherValue. Kept deliberately: declaring every property would mask
/// the general defect rather than fix it.
#[tokio::test]
async fn typed_and_undeclared_property_in_list_literal() {
    let db = fixture().await;
    let result = db
        .session()
        .query("MATCH (u:Uni) RETURN [u.name, u.undeclared] AS tuple")
        .await
        .unwrap();

    let items = list_items(result.rows()[0].value("tuple").unwrap());
    assert_eq!(items.len(), 2, "got {items:?}");
    assert_eq!(items[0], Value::String("Caltech".to_string()));
    assert_eq!(items[1], Value::String("x".to_string()));
}

/// The same list nested in a CASE branch — where the original report surfaced,
/// because `get_type` is called there first.
#[tokio::test]
async fn mixed_list_literal_inside_case() {
    let db = fixture().await;
    let result = db
        .session()
        .query(
            "MATCH (u:Uni) \
             RETURN CASE WHEN u.classYear > 2000 THEN [u.name, u.classYear] ELSE [] END AS tuple",
        )
        .await
        .unwrap();

    let items = list_items(result.rows()[0].value("tuple").unwrap());
    assert_eq!(items.len(), 2, "got {items:?}");
    assert_eq!(items[0], Value::String("Caltech".to_string()));
    assert_eq!(items[1], Value::Int(2011));
}

/// A uniformly-typed list must keep the native `make_array` path — the rewrite
/// is conditioned on mixed types and this is the guard on that condition.
#[tokio::test]
async fn uniform_typed_list_literal_is_unchanged() {
    let db = fixture().await;
    let result = db
        .session()
        .query("MATCH (u:Uni) RETURN [u.classYear, u.classYear] AS tuple")
        .await
        .unwrap();

    let items = list_items(result.rows()[0].value("tuple").unwrap());
    assert_eq!(items, vec![Value::Int(2011), Value::Int(2011)]);
}
