# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Python-side neural classifier registration for Locy `CREATE MODEL`.

These tests exercise the PyClassifier bridge that wraps a Python
callable as a `NeuralClassifier` Locy can dispatch to.

The contract under test:

- The callable receives `list[dict[str, Any]]` (one dict per row, keyed
  by the model's INPUT binding names). At a rule's call site `m(n.score)`
  the value is the argument expression; under `CALIBRATE` it is the model's
  FEATURES expression at the same position, so both see the same inputs.
- The callable returns `list[float]` of the same length, with values in
  [0, 1].
- Any Python exception, length mismatch, NaN, or out-of-range value
  surfaces as a Locy runtime error at the first invocation.
- Multiple classifiers can be registered against distinct aliases.
- A `CREATE MODEL ... USING xervo('alias')` referencing a missing alias
  fails at runtime with a clear "not registered" message.
"""

from __future__ import annotations

from typing import Any

import pytest

import uni_db

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


CONSTANT_RISK_PROGRAM = """
CREATE MODEL risk_scorer AS
  INPUT (n)
  FEATURES n.score
  OUTPUT PROB risk
  USING xervo('classify/risk-v1')

CREATE RULE risky AS
  MATCH (n:Asset)
  YIELD KEY n, risk_scorer(n.score) AS risk
"""


def _populated_asset_db():
    """Sync temporary db with five Asset nodes carrying a score."""
    db = uni_db.Uni.temporary()
    (
        db.schema()
        .label("Asset")
        .property("name", "string")
        .property("score", "float")
        .apply()
    )
    session = db.session()
    tx = session.tx()
    for name, score in [
        ("a1", 0.1),
        ("a2", 0.4),
        ("a3", 0.7),
        ("a4", 0.9),
        ("a5", 0.2),
    ]:
        tx.execute(f"CREATE (:Asset {{name: '{name}', score: {score}}})")
    tx.commit()
    return db


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_register_classifier_via_method_then_run_locy():
    """register_classifier on LocyConfig wires a Python callable into a
    CREATE MODEL invocation, and the rule yields the classifier's
    output."""
    db = _populated_asset_db()

    calls: list[list[dict[str, Any]]] = []

    def constant_half(inputs: list[dict[str, Any]]) -> list[float]:
        calls.append(inputs)
        return [0.5 for _ in inputs]

    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", constant_half)
    assert config.classifier_aliases() == ["risk_scorer"]
    # Note: the registry key is the CREATE MODEL name, not the
    # xervo(...) provider alias. `USING xervo('classify/...')` is a
    # provider hint; the runtime registry lookup uses the model name.

    session = db.session()
    result = session.locy_with(CONSTANT_RISK_PROGRAM).with_config(config).run()

    risky_rows = result.derived.get("risky", [])
    assert len(risky_rows) == 5, f"Expected 5 risky rows, got {len(risky_rows)}"
    for row in risky_rows:
        assert "risk" in row, f"row missing 'risk' field: {row}"
        assert row["risk"] == pytest.approx(0.5)

    assert len(calls) >= 1
    flat_inputs = [d for batch in calls for d in batch]
    assert len(flat_inputs) == 5
    # The feature dict is keyed by the INPUT binding name (here, "n").
    # The value is the evaluated argument expression at the invocation
    # site (here, n.score) -- a Float64 in [0, 1].
    for d in flat_inputs:
        assert "n" in d, f"feature dict missing 'n' key: {d}"
        assert isinstance(d["n"], (int, float)), f"feature value should be numeric: {d}"


def test_register_classifier_via_dict_config():
    """The same wiring works through a dict-form config passed to
    with_config — the codepath used when callers prefer a plain dict
    over the LocyConfig class."""
    db = _populated_asset_db()

    def per_score(inputs: list[dict[str, Any]]) -> list[float]:
        out: list[float] = []
        for row in inputs:
            # The feature dict is keyed by the INPUT binding name
            # (here, "n"). The value is the evaluated argument expression
            # at the call site -- here, n.score.
            s = row.get("n", 0.0)
            out.append(max(0.0, min(1.0, float(s))))
        return out

    session = db.session()
    result = (
        session.locy_with(CONSTANT_RISK_PROGRAM)
        .with_config({"classifier_registry": {"risk_scorer": per_score}})
        .run()
    )

    rows = sorted(
        result.derived.get("risky", []),
        key=lambda r: r["risk"],
    )
    risks = [r["risk"] for r in rows]
    assert risks == pytest.approx([0.1, 0.2, 0.4, 0.7, 0.9])


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_register_classifier_rejects_non_callable():
    config = uni_db.LocyConfig()
    with pytest.raises(TypeError, match="callable"):
        config.register_classifier("risk_scorer", 42)


def test_dict_form_rejects_non_callable_value():
    db = _populated_asset_db()
    session = db.session()
    with pytest.raises(TypeError, match="callable"):
        (
            session.locy_with(CONSTANT_RISK_PROGRAM)
            .with_config({"classifier_registry": {"risk_scorer": "not callable"}})
            .run()
        )


def test_missing_classifier_alias_fails_at_invocation():
    """If the program references xervo('classify/risk-v1') but the
    registry is empty, the Locy runtime fails with a clear
    "not registered" error at the first invocation."""
    db = _populated_asset_db()
    session = db.session()

    with pytest.raises(Exception) as exc_info:
        session.locy(CONSTANT_RISK_PROGRAM)

    msg = str(exc_info.value)
    assert "risk_scorer" in msg or "classifier" in msg.lower(), (
        f"missing-classifier error did not mention the alias: {msg!r}"
    )


def test_classifier_exception_surfaces_with_alias():
    """Exceptions from the Python callable should surface as a Locy
    error that includes the alias for debuggability."""
    db = _populated_asset_db()

    def boom(_inputs: list[dict[str, Any]]) -> list[float]:
        raise RuntimeError("intentional test failure inside classifier")

    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", boom)

    session = db.session()
    with pytest.raises(Exception) as exc_info:
        session.locy_with(CONSTANT_RISK_PROGRAM).with_config(config).run()

    msg = str(exc_info.value)
    assert "intentional test failure" in msg or "risk-v1" in msg, (
        f"classifier exception did not propagate clearly: {msg!r}"
    )


def test_classifier_arity_mismatch_surfaces():
    db = _populated_asset_db()

    def too_few(inputs: list[dict[str, Any]]) -> list[float]:
        # Return one fewer probability than inputs.
        return [0.5] * (len(inputs) - 1) if inputs else []

    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", too_few)

    session = db.session()
    with pytest.raises(Exception) as exc_info:
        session.locy_with(CONSTANT_RISK_PROGRAM).with_config(config).run()

    msg = str(exc_info.value).lower()
    assert "arity" in msg or "mismatch" in msg or "length" in msg, (
        f"arity mismatch error not surfaced clearly: {msg!r}"
    )


def test_classifier_nan_output_surfaces():
    db = _populated_asset_db()

    def nan_out(inputs: list[dict[str, Any]]) -> list[float]:
        return [float("nan") for _ in inputs]

    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", nan_out)

    session = db.session()
    with pytest.raises(Exception) as exc_info:
        session.locy_with(CONSTANT_RISK_PROGRAM).with_config(config).run()

    msg = str(exc_info.value).lower()
    assert "domain" in msg or "nan" in msg or "[0" in msg, (
        f"NaN output did not produce a domain error: {msg!r}"
    )


def test_classifier_out_of_range_output_surfaces():
    db = _populated_asset_db()

    def bad_range(inputs: list[dict[str, Any]]) -> list[float]:
        return [1.7 for _ in inputs]

    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", bad_range)

    session = db.session()
    with pytest.raises(Exception) as exc_info:
        session.locy_with(CONSTANT_RISK_PROGRAM).with_config(config).run()

    msg = str(exc_info.value).lower()
    assert "domain" in msg or "[0" in msg or "outside" in msg, (
        f"out-of-range output did not produce a domain error: {msg!r}"
    )


# ---------------------------------------------------------------------------
# Multiple classifiers
# ---------------------------------------------------------------------------


TWO_MODEL_PROGRAM = """
CREATE MODEL fast_risk AS
  INPUT (n)
  FEATURES (n.score)
  OUTPUT PROB risk
  USING xervo('classify/risk-fast')

CREATE MODEL slow_risk AS
  INPUT (n)
  FEATURES (n.score)
  OUTPUT PROB risk
  USING xervo('classify/risk-slow')

CREATE RULE risky_fast AS
  MATCH (n:Asset)
  YIELD KEY n, fast_risk(n) AS risk

CREATE RULE risky_slow AS
  MATCH (n:Asset)
  YIELD KEY n, slow_risk(n) AS risk
"""


def test_multiple_classifiers_register_independently():
    db = _populated_asset_db()

    def fast(inputs: list[dict[str, Any]]) -> list[float]:
        return [0.3 for _ in inputs]

    def slow(inputs: list[dict[str, Any]]) -> list[float]:
        return [0.8 for _ in inputs]

    config = uni_db.LocyConfig()
    config.register_classifier("fast_risk", fast)
    config.register_classifier("slow_risk", slow)
    assert config.classifier_aliases() == [
        "fast_risk",
        "slow_risk",
    ]

    session = db.session()
    result = session.locy_with(TWO_MODEL_PROGRAM).with_config(config).run()

    for row in result.derived.get("risky_fast", []):
        assert row["risk"] == pytest.approx(0.3)
    for row in result.derived.get("risky_slow", []):
        assert row["risk"] == pytest.approx(0.8)


def _recording(calls: list[dict[str, Any]]):
    def classify(inputs: list[dict[str, Any]]) -> list[float]:
        calls.extend(inputs)
        return [max(0.0, min(1.0, float(r["n"] or 0.0))) for r in inputs]

    return classify


def test_calibrate_passes_the_same_inputs_as_a_rule_call():
    """CALIBRATE gives the classifier the model's FEATURES values under the
    INPUT names, as a rule's call site does, never None."""
    db = _populated_asset_db()
    session = db.session()
    tx = session.tx()
    tx.execute("MATCH (n:Asset) SET n.bad = n.score > 0.5")
    tx.commit()

    rule_calls: list[dict[str, Any]] = []
    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", _recording(rule_calls))
    session.locy_with(CONSTANT_RISK_PROGRAM).with_config(config).run()

    calibrate_calls: list[dict[str, Any]] = []
    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", _recording(calibrate_calls))
    result = (
        session.locy_with(
            """
CREATE MODEL risk_scorer AS
  INPUT (n)
  FEATURES n.score
  OUTPUT PROB risk
  USING xervo('classify/risk-v1')

CALIBRATE risk_scorer
  ON MATCH (n:Asset)
  TARGET n.bad
  METHOD platt_scaling
  HOLDOUT 0.4
"""
        )
        .with_config(config)
        .run()
    )
    records = [
        c
        for c in result.command_results
        if isinstance(c, dict) and c.get("type") == "calibrate"
    ]
    assert len(records) == 1

    assert calibrate_calls, "CALIBRATE never called the classifier"
    assert all(r["n"] is not None for r in calibrate_calls), calibrate_calls
    assert sorted(r["n"] for r in calibrate_calls) == sorted(r["n"] for r in rule_calls)


def test_calibrate_passes_a_node_input_as_a_rule_call_does():
    """A model without FEATURES passes its INPUT variable: a node arrives
    as the same value under CALIBRATE as at a call site `m(n)`."""
    db = _populated_asset_db()
    session = db.session()
    tx = session.tx()
    tx.execute("MATCH (n:Asset) SET n.bad = n.score > 0.5")
    tx.commit()

    def recorder(calls: list[dict[str, Any]]):
        def classify(inputs: list[dict[str, Any]]) -> list[float]:
            calls.extend(inputs)
            return [0.5 for _ in inputs]

        return classify

    model = """
CREATE MODEL risk_scorer AS
  INPUT (n)
  OUTPUT PROB risk
  USING xervo('classify/risk-v1')
"""
    rule_calls: list[dict[str, Any]] = []
    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", recorder(rule_calls))
    session.locy_with(
        model
        + "\nCREATE RULE risky AS MATCH (n:Asset) YIELD KEY n, risk_scorer(n) AS risk"
    ).with_config(config).run()

    calibrate_calls: list[dict[str, Any]] = []
    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", recorder(calibrate_calls))
    session.locy_with(
        model + "\nCALIBRATE risk_scorer ON MATCH (n:Asset) TARGET n.bad "
        "METHOD platt_scaling HOLDOUT 0.4"
    ).with_config(config).run()

    assert all(r["n"] is not None for r in calibrate_calls), calibrate_calls
    assert sorted(r["n"] for r in calibrate_calls) == sorted(r["n"] for r in rule_calls)
