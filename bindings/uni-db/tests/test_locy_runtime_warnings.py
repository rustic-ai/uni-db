# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Locy runtime warnings as Python sees them.

Issue #315: a shared probabilistic dependency is reported by what the
engine did with it. Ordinary mode folds two proofs sharing one event as
independent (0.36 for the fixture, an overestimate) and warns; exact mode
resolves the shared event (0.20) and reports that as an informational
status, with structured fields an application can render without parsing
the message. ``has_warning`` matches a warning by its code.

Issue #314: a classifier-backed relation consumed through ``IS`` by a
second rule returns the classifier's score.
"""

from __future__ import annotations

import pytest

import uni_db

SHARED_EVENT_PROGRAM = """CREATE RULE factory_event AS
  MATCH (f:Facility {id: 'F1'})
  YIELD KEY f, 0.2 AS probability PROB
CREATE RULE exposed AS
  MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility)
  WHERE f IS factory_event
  YIELD KEY s, probability AS risk PROB
CREATE RULE portfolio AS
  MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility {id: 'F1'})
  WHERE s IS exposed
  FOLD p = MNOR(risk)
  YIELD KEY f, p AS probability PROB
QUERY portfolio RETURN f.id AS factory, probability
"""


def _run_shared(config: uni_db.LocyConfig):
    db = uni_db.Uni.temporary()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Facility {id:'F1'})")
    tx.execute(
        "MATCH (f:Facility {id:'F1'}) CREATE (:Supplier {id:'S1'})-[:DEPENDS_ON]->(f)"
    )
    tx.execute(
        "MATCH (f:Facility {id:'F1'}) CREATE (:Supplier {id:'S2'})-[:DEPENDS_ON]->(f)"
    )
    tx.commit()
    result = session.locy_with(SHARED_EVENT_PROGRAM).with_config(config).run()
    rows = result.rows()
    assert rows is not None and len(rows) == 1
    return rows[0]["probability"], result


def _for_portfolio(result, code: str):
    return [
        w
        for w in result.warnings
        if w["code"] == code and w["rule_name"] == "portfolio"
    ]


def test_ordinary_mode_warns_with_structured_diagnostic():
    p, result = _run_shared(
        uni_db.LocyConfig(exact_probability=False, strict_probability_domain=True)
    )
    assert p == pytest.approx(0.36)
    assert result.has_warning("shared_probabilistic_dependency")
    assert not result.has_warning("shared_dependency_resolved")
    (w,) = _for_portfolio(result, "shared_probabilistic_dependency")
    assert w["severity"] == "warning"
    d = w["probability"]
    assert d["inference_mode"] == "approximate"
    assert d["resolution"] == "not_attempted"
    assert d["reason"]
    assert d["groups_detected"] == 1


def test_exact_mode_reports_resolution_as_status():
    p, result = _run_shared(
        uni_db.LocyConfig(exact_probability=True, strict_probability_domain=True)
    )
    assert p == pytest.approx(0.20)
    assert not result.has_warning("shared_probabilistic_dependency")
    assert result.has_warning("shared_dependency_resolved")
    (w,) = _for_portfolio(result, "shared_dependency_resolved")
    assert w["severity"] == "info"
    d = w["probability"]
    assert d["inference_mode"] == "exact"
    assert d["resolution"] == "completed"
    assert d["groups_fallen_back"] == 0
    assert d["configured_bdd_variable_limit"] == 1000


def test_exact_mode_fallback_warns_with_reason_and_limits():
    p, result = _run_shared(
        uni_db.LocyConfig(
            exact_probability=True, strict_probability_domain=True, max_bdd_variables=1
        )
    )
    assert p == pytest.approx(0.36)
    assert result.has_warning("bdd_limit_exceeded")
    (w,) = _for_portfolio(result, "shared_probabilistic_dependency")
    d = w["probability"]
    assert d["inference_mode"] == "exact"
    assert d["resolution"] == "fallback"
    assert d["reason"] == "bdd_variable_limit_exceeded"
    assert d["configured_bdd_variable_limit"] == 1
    assert d["groups_fallen_back"] == 1


def test_classifier_relation_consumed_through_is():
    program = """CREATE MODEL delivery_model AS
 INPUT (n) FEATURES n.route_days OUTPUT PROB risk
 USING xervo('local/constant-v1') VERSION 'constant-v1'
CREATE RULE predicted AS
 MATCH (n:Supply)
 YIELD KEY n, delivery_model(n.route_days) AS probability PROB
CREATE RULE reviewed AS
 MATCH (r:Requirement)-[:USES]->(n:Supply)
 WHERE n IS predicted, probability >= 0.5
 YIELD KEY r, probability
QUERY reviewed RETURN r.id AS requirement, probability
"""
    db = uni_db.Uni.temporary()
    session = db.session()
    tx = session.tx()
    tx.execute(
        "CREATE (r:Requirement {id:'R'})-[:USES]->(n:Supply {id:'S',route_days:8})"
    )
    tx.commit()
    config = uni_db.LocyConfig(max_iterations=100, timeout_secs=20, allow_partial=False)
    config.register_classifier("delivery_model", lambda rows: [0.7 for _ in rows])
    result = session.locy_with(program).with_config(config).run()
    rows = result.rows()
    assert rows == [{"requirement": "R", "probability": pytest.approx(0.7)}]
