# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""User properties named like entity fields, and COPY TO, from Python.

A property called ``label``, ``labels``, ``type`` or ``properties`` is an
ordinary property: it appears in ``Node.properties`` / ``Edge.properties``,
dict access and ``keys()``, while ``.labels`` / ``.type`` / ``.properties``
stay the entity's own fields. ``COPY ... TO`` writes the same property set
``properties()`` returns.
"""

import csv

import pytest

import uni_db

COLLIDING = ["label", "labels", "type", "properties"]
PROPS = {k: f"{k}-value" for k in COLLIDING} | {"name": "x"}
LITERAL = "{" + ", ".join(f"{k}: '{v}'" for k, v in PROPS.items()) + "}"


def _seed(db):
    schema = db.schema().label("D")
    for k in [*COLLIDING, "name"]:
        schema = schema.property_nullable(k, "string")
    edge = schema.done().edge_type("E", ["D"], ["D"])
    for k in [*COLLIDING, "name"]:
        edge = edge.property_nullable(k, "string")
    edge.apply()
    tx = db.session().tx()
    tx.execute(
        f"CREATE (:D {LITERAL})-[:E {LITERAL}]->(:D {LITERAL}), "
        f"(:S {LITERAL})-[:R {LITERAL}]->(:S {LITERAL})"
    )
    tx.commit()


@pytest.mark.parametrize("flush", [False, True], ids=["l0", "flushed"])
def test_colliding_property_names_reach_python(flush):
    db = uni_db.Uni.temporary()
    _seed(db)
    if flush:
        db.flush()
    session = db.session()
    for pattern, var, kind in [
        ("(n:D)", "n", uni_db.Node),
        ("(n:S)", "n", uni_db.Node),
        ("(:D)-[r:E]->(:D)", "r", uni_db.Edge),
        ("(:S)-[r:R]->(:S)", "r", uni_db.Edge),
    ]:
        row = session.query(
            f"MATCH {pattern} RETURN {var} AS e, properties({var}) AS p, "
            f"keys({var}) AS k, {var}.label AS l, {var}.type AS t LIMIT 1"
        )[0]
        entity = row["e"]
        assert isinstance(entity, kind)
        assert entity.properties == PROPS, pattern
        assert row["p"] == PROPS, pattern
        assert sorted(row["k"]) == sorted(PROPS), pattern
        assert row["l"] == "label-value"
        assert row["t"] == "type-value"
        for k, v in PROPS.items():
            assert entity[k] == v
            assert k in entity
        if kind is uni_db.Node:
            assert entity.labels in (["D"], ["S"])
        else:
            assert entity.type in ("E", "R")


def test_copy_to_csv_carries_undeclared_properties(tmp_path):
    db = uni_db.Uni.temporary()
    tx = db.session().tx()
    tx.execute("CREATE (:P {a: 1, label: 'l'}), (:P {b: 'two'})")
    tx.commit()
    out = tmp_path / "p.csv"
    tx = db.session().tx()
    tx.execute(f"COPY P TO '{out}'")
    tx.commit()
    with out.open() as f:
        rows = list(csv.DictReader(f))
    assert list(rows[0].keys()) == ["_vid", "a", "b", "label"]
    got = sorted((r["a"], r["b"], r["label"]) for r in rows)
    assert got == [("", "two", ""), ("1", "", "l")]
