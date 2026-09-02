//! `datetime({epochMillis: …})` and `datetime({epochSeconds: …})`.
//!
//! Constructing a temporal from an epoch offset is the natural thing to do when
//! the graph stores timestamps as integers, and it is how LDBC SNB Interactive
//! IC10 reads a person's birthday:
//!
//! ```cypher
//! WITH person, city, friend, datetime({epochMillis: friend.birthday}) AS birthday
//! WHERE (birthday.month = $month AND birthday.day >= 21) OR …
//! ```
//!
//! The map constructor required a `year` field, so every epoch form failed with
//! `datetime(): date/datetime map requires 'year' field`.

use uni_db::{Uni, Value};

async fn db() -> Uni {
    Uni::in_memory().build().await.unwrap()
}

async fn one(db: &Uni, q: &str) -> Value {
    let r = db.session().query(q).await.unwrap();
    r.rows()[0].values()[0].clone()
}

#[tokio::test]
async fn epoch_millis_at_the_epoch_itself() {
    let db = db().await;
    assert_eq!(
        one(&db, "RETURN datetime({epochMillis: 0}).year").await,
        Value::Int(1970)
    );
    assert_eq!(
        one(&db, "RETURN datetime({epochMillis: 0}).month").await,
        Value::Int(1)
    );
    assert_eq!(
        one(&db, "RETURN datetime({epochMillis: 0}).day").await,
        Value::Int(1)
    );
}

/// 2009-02-13T23:31:30.123Z — a value with a non-zero sub-second part, so the
/// millisecond component is not silently dropped.
#[tokio::test]
async fn epoch_millis_round_trips_a_known_instant() {
    let db = db().await;
    for (accessor, expected) in [
        ("year", 2009),
        ("month", 2),
        ("day", 13),
        ("hour", 23),
        ("minute", 31),
        ("second", 30),
        ("millisecond", 123),
    ] {
        assert_eq!(
            one(
                &db,
                &format!("RETURN datetime({{epochMillis: 1234567890123}}).{accessor}")
            )
            .await,
            Value::Int(expected),
            "accessor {accessor}"
        );
    }
}

/// Before the epoch, where truncating division would round the wrong way.
#[tokio::test]
async fn epoch_millis_before_the_epoch() {
    let db = db().await;
    assert_eq!(
        one(&db, "RETURN datetime({epochMillis: -1}).year").await,
        Value::Int(1969)
    );
    assert_eq!(
        one(&db, "RETURN datetime({epochMillis: -1}).millisecond").await,
        Value::Int(999)
    );
}

#[tokio::test]
async fn epoch_seconds_with_a_nanosecond_component() {
    let db = db().await;
    assert_eq!(
        one(&db, "RETURN datetime({epochSeconds: 1234567890}).year").await,
        Value::Int(2009)
    );
    assert_eq!(
        one(
            &db,
            "RETURN datetime({epochSeconds: 1234567890, nanosecond: 500000000}).millisecond"
        )
        .await,
        Value::Int(500)
    );
}

/// `localdatetime` takes the same map; the epoch is UTC.
#[tokio::test]
async fn local_datetime_from_epoch_millis() {
    let db = db().await;
    assert_eq!(
        one(
            &db,
            "RETURN localdatetime({epochMillis: 1234567890123}).hour"
        )
        .await,
        Value::Int(23)
    );
}

/// IC10's shape: the epoch value comes from a stored property, not a literal.
#[tokio::test]
async fn epoch_millis_from_a_property() {
    let db = db().await;
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING, birthday INT)")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a', birthday: 1234567890123})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let r = db
        .session()
        .query(
            "MATCH (p:P) WITH datetime({epochMillis: p.birthday}) AS b \
             RETURN b.month AS m, b.day AS d",
        )
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(2));
    assert_eq!(r.rows()[0].values()[1], Value::Int(13));
}

/// A map with neither an epoch field nor `year` is still an error — the epoch
/// branch must not turn a genuinely malformed map into a silent default.
#[tokio::test]
async fn a_map_with_no_epoch_and_no_year_is_still_an_error() {
    let db = db().await;
    let err = db
        .session()
        .query("RETURN datetime({month: 3})")
        .await
        .expect_err("a datetime map needs a year or an epoch field");
    assert!(
        format!("{err}").contains("year"),
        "expected the missing-year error, got: {err}"
    );
}
