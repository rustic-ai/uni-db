// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.degreeCentrality procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{
    Algorithm, DegreeCentrality, DegreeCentralityConfig, DegreeDirection,
};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, vid_pair_rows};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};

pub struct DegreeCentralityAdapter;

impl GraphAlgoAdapter for DegreeCentralityAdapter {
    const NAME: &'static str = "uni.algo.degreeCentrality";
    type Algo = DegreeCentrality;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("direction", ValueType::String, Some(json!("OUTGOING")))]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("score", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> Result<DegreeCentralityConfig> {
        let direction = parse_direction(args[0].as_str().unwrap_or("OUTGOING"))?;

        Ok(DegreeCentralityConfig { direction })
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(vid_pair_rows(result.scores))
    }

    fn customize_projection(builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
        // An unknown spelling used to land here as OUTGOING *and* as
        // include_reverse(false), so a typo'd `direction:'INCOMMING'` reported
        // outgoing degrees; `to_config` now rejects it before the projection is
        // built, and a genuine INCOMING/BOTH still gets the reverse CSR.
        builder.include_reverse(include_reverse_for(args[0].as_str().unwrap_or("OUTGOING")))
    }
}

/// Parse the `direction` argument, rejecting anything that is not a known
/// spelling instead of defaulting to OUTGOING.
fn parse_direction(raw: &str) -> Result<DegreeDirection> {
    match raw.to_uppercase().as_str() {
        "OUTGOING" | "OUT" => Ok(DegreeDirection::Outgoing),
        "INCOMING" | "IN" => Ok(DegreeDirection::Incoming),
        "BOTH" => Ok(DegreeDirection::Both),
        other => Err(anyhow!(
            "unknown `direction` {other:?} for uni.algo.degreeCentrality; \
             accepted values are OUTGOING, OUT, INCOMING, IN, BOTH (case-insensitive)"
        )),
    }
}

/// Whether `direction` needs the reverse (in-neighbor) CSR.
///
/// An unknown spelling answers `false`; `to_config` rejects it first, so this
/// is never the path a typo takes to a wrong (all-zero incoming) answer.
fn include_reverse_for(raw: &str) -> bool {
    matches!(
        parse_direction(raw),
        Ok(DegreeDirection::Incoming | DegreeDirection::Both)
    )
}

pub type DegreeCentralityProcedure = GenericAlgoProcedure<DegreeCentralityAdapter>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_direction_errors_instead_of_defaulting_to_outgoing() {
        // A3: `direction:'INCOMMING'` used to return OUTGOING degrees, and it
        // also switched off the reverse CSR so real incoming degrees read 0.
        for (name, want) in [
            ("OUTGOING", DegreeDirection::Outgoing),
            ("incoming", DegreeDirection::Incoming),
            ("In", DegreeDirection::Incoming),
            ("BOTH", DegreeDirection::Both),
        ] {
            let cfg = DegreeCentralityAdapter::to_config(vec![json!(name)])
                .unwrap_or_else(|e| panic!("{name} must parse: {e}"));
            assert_eq!(cfg.direction, want, "direction {name}");
        }

        let err = DegreeCentralityAdapter::to_config(vec![json!("INCOMMING")])
            .expect_err("a typo'd direction must error");
        let msg = err.to_string();
        assert!(
            msg.contains("INCOMMING"),
            "error must echo the input: {msg}"
        );
        assert!(
            msg.contains("INCOMING") && msg.contains("BOTH"),
            "error must list the accepted values: {msg}"
        );
    }

    #[test]
    fn incoming_direction_still_builds_the_reverse_csr() {
        // The include_reverse half of the same fail-open: without it, genuine
        // incoming degrees come back 0.
        for (name, want) in [
            ("INCOMING", true),
            ("IN", true),
            ("BOTH", true),
            ("OUTGOING", false),
        ] {
            assert_eq!(include_reverse_for(name), want, "direction {name}");
        }
    }
}
