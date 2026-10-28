// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `ABDUCE NOT` candidates edit the one entity a derivation read.
//!
//! A candidate is validated by applying it hypothetically and re-deriving.
//! The mutation each candidate was applied through named no entity:
//! `RemoveEdge` ran `MATCH (a)-[e:R]->(b) DELETE e`, deleting every `R` edge,
//! and `ChangeProperty` ran `MATCH (n) SET n.k = 0`, setting `k` on every node
//! (and, for an edge variable, on every node instead of the edge). Validation
//! therefore judged a far larger edit than the one reported, accepting
//! removals that do not change the answer. The evidence row's exploded
//! helper columns were also read as variables, proposing every property a
//! second time under `a._all_props`.

use anyhow::{Result, bail};
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyConfig};
use uni_locy::{AbductionResult, Modification};

async fn abduce(db: &Uni, program: &str) -> Result<AbductionResult> {
    let result = db
        .session()
        .locy_with(program)
        .with_config(LocyConfig::default())
        .run()
        .await?;
    for cr in &result.command_results {
        if let CommandResult::Abduce(ab) = cr {
            return Ok(ab.clone());
        }
    }
    bail!("no ABDUCE result")
}

async fn seed(db: &Uni, cypher: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    Ok(())
}

/// `A1` has two `R` edges, so removing either one leaves `t(A1)` derived:
/// no single removal satisfies `ABDUCE NOT`. Deleting every `R` edge did,
/// and was reported as removing one.
async fn assert_no_single_removal_validates(body: &str) -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(
        &db,
        "CREATE (a:N {name: 'A1'}), (b:N {name: 'B1'}), (c:N {name: 'B2'}), \
         (a)-[:R]->(b), (a)-[:R]->(c)",
    )
    .await?;
    let program = format!(
        "CREATE RULE t AS MATCH {body} YIELD KEY a \n\
         ABDUCE NOT t WHERE a.name = 'A1'"
    );
    let ab = abduce(&db, &program).await?;
    let removals: Vec<_> = ab
        .modifications
        .iter()
        .filter(|m| matches!(m.modification, Modification::RemoveEdge { .. }))
        .collect();
    if removals.is_empty() {
        bail!("no RemoveEdge candidate for {body}: {:?}", ab.modifications);
    }
    for m in &removals {
        if m.validated {
            bail!(
                "{body}: a single-edge removal cannot remove t(A1), yet {:?} validated",
                m.modification
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn abduce_remove_named_edge_removes_only_that_edge() -> Result<()> {
    assert_no_single_removal_validates("(a:N)-[e:R]->(b:N)").await
}

#[tokio::test]
async fn abduce_remove_anonymous_edge_removes_only_that_edge() -> Result<()> {
    assert_no_single_removal_validates("(a:N)-[:R]->(b:N)").await
}

/// The written direction does not matter: the removal is pinned either way.
#[tokio::test]
async fn abduce_remove_incoming_edge_removes_only_that_edge() -> Result<()> {
    assert_no_single_removal_validates("(b:N)<-[:R]-(a:N)").await
}

/// `t(a)` holds while `b.k < a.k`. Zeroing `a.k` breaks it; zeroing `b.k`
/// does not. Applied to every node, zeroing `b.k` also zeroed `a.k`, and
/// was accepted.
#[tokio::test]
async fn abduce_change_property_changes_only_that_entity() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(
        &db,
        "CREATE (:N {name: 'a', k: 5})-[:R]->(:N {name: 'b', k: 3})",
    )
    .await?;
    let ab = abduce(
        &db,
        "CREATE RULE t AS MATCH (a:N)-[:R]->(b:N) WHERE b.k < a.k YIELD KEY a \n\
         ABDUCE NOT t",
    )
    .await?;
    let mut seen = Vec::new();
    for m in &ab.modifications {
        if let Modification::ChangeProperty {
            element_var,
            property,
            ..
        } = &m.modification
        {
            if element_var.contains('.') {
                bail!("candidate names a helper column: {:?}", m.modification);
            }
            let id = (element_var.clone(), property.clone());
            if seen.contains(&id) {
                bail!("duplicate candidate {id:?}: {:?}", ab.modifications);
            }
            seen.push(id);
        }
    }
    let validated = |var: &str| {
        ab.modifications.iter().find_map(|m| match &m.modification {
            Modification::ChangeProperty {
                element_var,
                property,
                ..
            } if element_var == var && property == "k" => Some(m.validated),
            _ => None,
        })
    };
    if validated("a") != Some(true) {
        bail!("zeroing a.k breaks t(a): {:?}", ab.modifications);
    }
    if validated("b") != Some(false) {
        bail!("zeroing b.k alone keeps t(a): {:?}", ab.modifications);
    }
    Ok(())
}

/// A property of an edge variable is changed on that edge. It was set on
/// every node instead, so the edge kept its value and the candidate failed.
#[tokio::test]
async fn abduce_change_edge_property_changes_the_edge() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(&db, "CREATE (:N {name: 'a'})-[:R {w: 3}]->(:N {name: 'b'})").await?;
    let ab = abduce(
        &db,
        "CREATE RULE t AS MATCH (a:N)-[e:R]->(b:N) WHERE e.w > 1 YIELD KEY a \n\
         ABDUCE NOT t",
    )
    .await?;
    let edge_change = ab.modifications.iter().find(|m| {
        matches!(
            &m.modification,
            Modification::ChangeProperty { element_var, property, eid: Some(_), .. }
                if element_var == "e" && property == "w"
        )
    });
    match edge_change {
        Some(m) if m.validated => Ok(()),
        _ => bail!("zeroing e.w breaks t(a): {:?}", ab.modifications),
    }
}

/// Positive ABDUCE: a single added edge is validated as that one edge.
///
/// `t(A, _)` needs two `R` hops, so no single `R` edge derives it. Each
/// `AddEdge` candidate is reported as one edge, so none may validate.
#[tokio::test]
async fn abduce_add_edge_validates_one_edge_not_every_pair() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(
        &db,
        "CREATE (:N {name: 'A'}), (:N {name: 'B'}), (:N {name: 'C'})",
    )
    .await?;
    let ab = abduce(
        &db,
        "CREATE RULE t AS MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) YIELD KEY a, KEY c \n\
         ABDUCE t WHERE a.name = 'A'",
    )
    .await?;
    eprintln!("positive ABDUCE: {:#?}", ab.modifications);
    let adds: Vec<_> = ab
        .modifications
        .iter()
        .filter(|m| matches!(m.modification, Modification::AddEdge { .. }))
        .collect();
    if adds.is_empty() {
        bail!("no AddEdge candidate: {:?}", ab.modifications);
    }
    if let Some(m) = adds.iter().find(|m| m.validated) {
        bail!(
            "a single R edge cannot derive t(A, _), yet {:?} validated",
            m.modification
        );
    }
    Ok(())
}

/// Positive ABDUCE proposes concrete edges, and validates the one that
/// derives the goal: `A -> C`, not `A -> B`.
#[tokio::test]
async fn abduce_add_edge_proposes_concrete_endpoints() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(
        &db,
        "CREATE (:N {name: 'A'}), (:N {name: 'B'}), (:N {name: 'C'})",
    )
    .await?;
    let ids = db
        .session()
        .query("MATCH (n:N) RETURN n.name AS name, id(n) AS id")
        .await?;
    let id_of = |name: &str| -> Option<u64> {
        ids.rows()
            .iter()
            .find(|r| r.get::<String>("name").ok().as_deref() == Some(name))
            .and_then(|r| r.get::<i64>("id").ok())
            .map(|i| i as u64)
    };
    let ab = abduce(
        &db,
        "CREATE RULE t AS MATCH (a:N)-[:R]->(b:N) WHERE b.name = 'C' YIELD KEY a, KEY b \n\
         ABDUCE t WHERE a.name = 'A'",
    )
    .await?;
    let validated: Vec<(Option<u64>, Option<u64>)> = ab
        .modifications
        .iter()
        .filter(|m| m.validated)
        .filter_map(|m| match &m.modification {
            Modification::AddEdge {
                source_vid,
                target_vid,
                ..
            } => Some((
                source_vid.map(|v| v.as_u64()),
                target_vid.map(|v| v.as_u64()),
            )),
            _ => None,
        })
        .collect();
    if validated != vec![(id_of("A"), id_of("C"))] {
        bail!(
            "only the edge A -> C derives t(A, C); validated {validated:?} of {:?}",
            ab.modifications
        );
    }
    Ok(())
}

/// An anonymous endpoint node still identifies the edge, by the endpoints'
/// vids: the removal is proposed, and pinned to that one edge.
#[tokio::test]
async fn abduce_remove_edge_into_anonymous_node_removes_only_that_edge() -> Result<()> {
    assert_no_single_removal_validates("(a:N)-[:R]->(:N)").await
}

/// A body whose middle node is anonymous proposes the edges on both sides of
/// it. The derivation's evidence row bound only named elements, so neither
/// hop could be identified and ABDUCE NOT returned no candidate at all.
#[tokio::test]
async fn abduce_through_anonymous_middle_node_proposes_both_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(
        &db,
        "CREATE (:L {id: 'l1'})-[:OBSERVED]->(:F {id: 'f1'})-[:ON]->(:T {id: 't1'})",
    )
    .await?;
    let ab = abduce(
        &db,
        "CREATE RULE q AS MATCH (l:L)-[:OBSERVED]->(:F)-[:ON]->(t:T) YIELD KEY l \n\
         ABDUCE NOT q WHERE l.id = 'l1'",
    )
    .await?;
    let mut removed = Vec::new();
    for m in &ab.modifications {
        let Modification::RemoveEdge {
            edge_type,
            source_var,
            target_var,
            edge_var,
            ..
        } = &m.modification
        else {
            continue;
        };
        for var in [source_var, target_var, edge_var] {
            if var.starts_with("abduce_anon_") {
                bail!("a generated variable leaked into {:?}", m.modification);
            }
        }
        if !m.validated {
            bail!("removing {edge_type} breaks q(l1), yet it did not validate");
        }
        removed.push(edge_type.clone());
    }
    removed.sort();
    if removed != ["OBSERVED", "ON"] {
        bail!("expected both hops proposed, got {:?}", ab.modifications);
    }
    Ok(())
}

/// IS children explain the referenced rule, not the parent's own MATCH edge.
#[tokio::test]
async fn abduce_not_keeps_parent_match_edges_with_is_dependencies() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(
        &db,
        "CREATE (:N {name: 'a'})-[:R]->(:N {name: 'b', score: 1})",
    )
    .await?;
    let result = abduce(
        &db,
        "CREATE RULE leaf AS MATCH (b:N) WHERE b.name = 'b' YIELD KEY b \n\
         CREATE RULE target AS MATCH (a:N)-[:R]->(b:N) WHERE b IS leaf YIELD KEY a \n\
         ABDUCE NOT target WHERE a.name = 'a'",
    )
    .await?;
    assert!(
        result.modifications.iter().any(|m| m.validated
            && matches!(
                &m.modification, Modification::RemoveEdge { edge_type, .. } if edge_type == "R"
            )),
        "the single supporting parent edge must be removable: {result:?}"
    );
    let rows = db
        .session()
        .query("MATCH ()-[r:R]->() RETURN count(r) AS n")
        .await?;
    assert_eq!(
        rows.rows()[0].get::<i64>("n")?,
        1,
        "validation must roll back its edit"
    );
    Ok(())
}

/// Aggregate evidence can itself have IS children: the PROTECTS edge still
/// supports the risk fact even though the control's weakness is a leaf rule.
#[tokio::test]
async fn abduce_not_keeps_aggregate_match_edges_with_is_dependencies() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(
        &db,
        "CREATE (:Control {effectiveness: 0.2})-[:PROTECTS]->(:System {name: 'target'})",
    )
    .await?;
    let result = abduce(
        &db,
        "CREATE RULE weak AS MATCH (c:Control) WHERE c.effectiveness < 0.5 YIELD KEY c \n\
         CREATE RULE risk AS MATCH (c:Control)-[:PROTECTS]->(s:System)          WHERE c IS weak FOLD total = MNOR(1.0 - c.effectiveness) YIELD KEY s, total \n\
         CREATE RULE unacceptable AS MATCH (s:System) WHERE s IS risk, total >= 0.5 YIELD KEY s \n\
         ABDUCE NOT unacceptable WHERE s.name = 'target'",
    ).await?;
    assert!(result.modifications.iter().any(|m| m.validated && matches!(
        &m.modification, Modification::RemoveEdge { edge_type, .. } if edge_type == "PROTECTS"
    )), "the aggregate's supporting edge must be removable: {result:?}");
    let rows = db
        .session()
        .query("MATCH ()-[r:PROTECTS]->() RETURN count(r) AS n")
        .await?;
    assert_eq!(
        rows.rows()[0].get::<i64>("n")?,
        1,
        "validation must roll back its edit"
    );
    Ok(())
}
