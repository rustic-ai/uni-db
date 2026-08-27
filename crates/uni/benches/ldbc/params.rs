// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Derive substitution parameters from the loaded graph.
//!
//! LDBC ships example parameters in each query's header comment, but they name
//! ids from LDBC's own micro dataset — against SF1 they mostly select nothing.
//! A query returning zero rows would still "agree" with an oracle returning zero
//! rows, so parameters that select nothing turn the whole differential check
//! vacuous. These are therefore derived from the data actually loaded, chosen to
//! be *busy* rather than arbitrary: the most-connected person, the most-used
//! tag, the countries with the most residents.
//!
//! Being busy is a heuristic, not a guarantee. The runner still asserts every
//! query returned rows, and that assertion is what actually protects the result.

use std::collections::HashMap;

use uni_db::{Uni, Value};

/// One scalar from a query, or `None` if it returned no rows.
async fn scalar(db: &Uni, cypher: &str) -> anyhow::Result<Option<Value>> {
    let rows = db.session().query(cypher).await?;
    Ok(rows
        .rows()
        .first()
        .and_then(|r| r.values().first().cloned()))
}

fn need(v: Option<Value>, what: &str) -> anyhow::Result<Value> {
    v.ok_or_else(|| {
        anyhow::anyhow!(
            "could not derive {what} from the loaded graph — the load is probably incomplete, \
             and running with a guessed value would make the comparison vacuous"
        )
    })
}

/// Build the full substitution-parameter set.
pub async fn derive(db: &Uni) -> anyhow::Result<HashMap<String, Value>> {
    let mut p: HashMap<String, Value> = HashMap::new();

    // The most-connected person: maximises the chance that friend-traversal
    // queries (IC1-IC14 nearly all start from a person) reach anything.
    let hub = need(
        scalar(
            db,
            "MATCH (p:Person)-[:KNOWS]-() WITH p, count(*) AS d \
             RETURN p.id AS id ORDER BY d DESC LIMIT 1",
        )
        .await?,
        "a hub person",
    )?;
    p.insert("personId".to_string(), hub.clone());
    p.insert("person1Id".to_string(), hub.clone());

    // A second person reachable from the hub, so IC13/IC14's path queries have a
    // path to find. Picked at 2..3 hops rather than 1 so the answer is not
    // trivially the direct edge.
    let hub_id = as_i64(&hub);
    let p2 = need(
        scalar(
            db,
            &format!(
                "MATCH (a:Person {{id: {hub_id}}})-[:KNOWS*2..3]-(b:Person) \
                 WHERE a <> b RETURN b.id AS id LIMIT 1"
            ),
        )
        .await?,
        "a second person reachable from the hub",
    )?;
    p.insert("person2Id".to_string(), p2);

    // A first name that actually occurs among the hub's 3-hop neighbourhood,
    // which is exactly the set IC1 searches.
    let first_name = need(
        scalar(
            db,
            &format!(
                "MATCH (a:Person {{id: {hub_id}}})-[:KNOWS*1..3]-(f:Person) \
                 WITH f.firstName AS n, count(*) AS c \
                 RETURN n ORDER BY c DESC LIMIT 1"
            ),
        )
        .await?,
        "a first name in the hub's neighbourhood",
    )?;
    p.insert("firstName".to_string(), first_name);

    // Date window over the actual message range. `maxDate`/`minDate` are set
    // wide so the filters admit most of the corpus rather than a thin slice.
    let min_date = as_i64(&need(
        scalar(db, "MATCH (m:Post) RETURN min(m.creationDate) AS d").await?,
        "the earliest post date",
    )?);
    let max_date = as_i64(&need(
        scalar(db, "MATCH (m:Post) RETURN max(m.creationDate) AS d").await?,
        "the latest post date",
    )?);
    let span = (max_date - min_date).max(1);
    p.insert("minDate".to_string(), Value::Int(min_date));
    p.insert("maxDate".to_string(), Value::Int(max_date));
    p.insert("startDate".to_string(), Value::Int(min_date));
    // IC3/IC4 filter to a window `[startDate, startDate + duration)`; using the
    // whole span keeps them non-empty.
    p.insert("endDate".to_string(), Value::Int(min_date + span));
    p.insert(
        "durationDays".to_string(),
        Value::Int(span / 86_400_000 + 1),
    );

    // The most-used tag and the tag class covering the most tags.
    let tag = need(
        scalar(
            db,
            "MATCH (t:Tag)<-[:HAS_TAG]-() WITH t, count(*) AS c \
             RETURN t.name AS n ORDER BY c DESC LIMIT 1",
        )
        .await?,
        "the most-used tag",
    )?;
    p.insert("tagName".to_string(), tag);

    let tag_class = need(
        scalar(
            db,
            "MATCH (tc:TagClass)<-[:HAS_TYPE]-(t:Tag) WITH tc, count(t) AS c \
             RETURN tc.name AS n ORDER BY c DESC LIMIT 1",
        )
        .await?,
        "the largest tag class",
    )?;
    p.insert("tagClassName".to_string(), tag_class);

    // The two countries with the most residents — IC3 compares activity across
    // a pair, so both need to be populated.
    let countries = db
        .session()
        .query(
            "MATCH (c:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(p:Person) \
             WITH c, count(p) AS n RETURN c.name AS n2 ORDER BY n DESC LIMIT 2",
        )
        .await?;
    let names: Vec<Value> = countries
        .rows()
        .iter()
        .filter_map(|r| r.values().first().cloned())
        .collect();
    anyhow::ensure!(
        names.len() >= 2,
        "need two populated countries for IC3; found {}",
        names.len()
    );
    p.insert("countryXName".to_string(), names[0].clone());
    p.insert("countryYName".to_string(), names[1].clone());
    // IC11 filters on a single country.
    p.insert("countryName".to_string(), names[0].clone());

    // A work-start year late enough that `workFrom < $workFromYear` admits rows.
    let work_year = need(
        scalar(db, "MATCH ()-[w:WORK_AT]->() RETURN max(w.workFrom) AS y").await?,
        "a WORK_AT year",
    )?;
    p.insert(
        "workFromYear".to_string(),
        Value::Int(as_i64(&work_year) + 1),
    );

    // IC10 filters on birthday month; pick the most common one.
    let month = need(
        scalar(
            db,
            "MATCH (p:Person) WITH p.birthday AS b WHERE b IS NOT NULL \
             RETURN b AS m LIMIT 1",
        )
        .await?,
        "a birthday",
    )?;
    // `birthday` is epoch millis; month is 1-12 derived from it.
    let month_num = epoch_ms_to_month(as_i64(&month));
    p.insert("month".to_string(), Value::Int(month_num));

    Ok(p)
}

fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i,
        Value::Float(f) => *f as i64,
        Value::String(s) => s.parse().unwrap_or_default(),
        _ => 0,
    }
}

/// Month (1-12) of an epoch-millisecond timestamp, via days-since-epoch and the
/// civil-from-days algorithm — no chrono dependency needed for one field.
fn epoch_ms_to_month(ms: i64) -> i64 {
    let days = ms.div_euclid(86_400_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    if mp < 10 { mp + 3 } else { mp - 9 }
}
