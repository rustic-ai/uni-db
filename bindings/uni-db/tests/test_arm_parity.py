"""The Cypher and Locy arms must expose the same knobs, and carry the same metrics.

Three gaps closed here, all of the same shape -- a capability present on one
arm or one scope and silently absent on another:

* ``LocyResult`` had no ``metrics`` while ``QueryResult`` did, so an identical
  workload yielded timing and scan counters through Cypher and nothing through
  Locy. The Rust wrapper had carried them all along; the converter threw them
  away on its first line.
* ``max_memory`` was missing on the transaction Cypher builder -- the only one
  of {session, tx} x {Cypher, Locy} without it, and the shape that needs a
  ceiling most: a long-running read inside a write transaction.
* ``profile`` was missing there too, so a *read* inside a transaction could not
  be profiled at all.

A fourth was found while fixing those: ``profile()`` accepted ``max_memory``
and dropped it on the floor, on both the session and the (new) tx path.
"""

from __future__ import annotations

import pytest

import uni_db

CYCLIC = "MATCH p=(a:E {uid:'e0'})-[:OWNS*1..6]->(b:E) RETURN count(p) AS n"
TINY = 64 * 1024
ROOMY = 512 * 1024 * 1024


@pytest.fixture
def db(tmp_path):
    """A small strongly-connected graph -- enough paths to exceed a tiny pool."""
    database = uni_db.Uni.open(str(tmp_path / "g"))
    (
        database.schema()
        .label("E")
        .property("uid", "string")
        .done()
        .edge_type("OWNS", ["E"], ["E"])
        .done()
        .apply()
    )
    session = database.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()
    n, out = 30, 3
    vids = list(writer.insert_vertices("E", [{"uid": f"e{i}"} for i in range(n)]))
    writer.insert_edges(
        "OWNS",
        [
            (vids[i], vids[(i * 7 + k * 13 + 1) % n], {})
            for i in range(n)
            for k in range(out)
        ],
    )
    writer.commit()
    tx.commit()
    return database


# --- the knob matrix --------------------------------------------------------


@pytest.mark.parametrize(
    "builder",
    ["SessionQueryBuilder", "TxQueryBuilder", "SessionLocyBuilder", "TxLocyBuilder"],
)
@pytest.mark.parametrize("knob", ["max_memory", "profile"])
def test_every_arm_and_scope_exposes_the_knob(builder, knob):
    """All four cells filled, for both knobs.

    Parameterised over the whole matrix rather than asserting only the two that
    were missing: a matrix with a hole is how this arose, and a test that checks
    only the known hole cannot see the next one.
    """
    cls = getattr(uni_db, builder, None)
    assert cls is not None, f"uni_db.{builder} does not exist"
    assert hasattr(cls, knob), f"{builder} is missing .{knob}()"


@pytest.mark.parametrize(
    "builder",
    [
        "AsyncSessionQueryBuilder",
        "AsyncTxQueryBuilder",
        "AsyncSessionLocyBuilder",
        "AsyncTxLocyBuilder",
    ],
)
@pytest.mark.parametrize("knob", ["max_memory", "profile"])
def test_the_async_twins_match(builder, knob):
    cls = getattr(uni_db, builder, None)
    assert cls is not None, f"uni_db.{builder} does not exist"
    assert hasattr(cls, knob), f"{builder} is missing .{knob}()"


# --- the knobs actually bind ------------------------------------------------


def test_tx_max_memory_refuses_a_query_that_does_not_fit(db):
    session = db.session()
    # The type is what separates a ceiling from a crash: a refusal for cost is
    # `UniMemoryLimitExceededError`, never the `UniQueryError` a broken query
    # raises (#289). This used to catch `Exception` and match the message.
    with pytest.raises(uni_db.UniMemoryLimitExceededError):
        session.tx().query_with(CYCLIC).max_memory(TINY).fetch_all()


def test_tx_max_memory_leaves_a_fitting_query_alone(db):
    """The control. Without it, a build that refused everything would pass."""
    session = db.session()
    result = session.tx().query_with(CYCLIC).max_memory(ROOMY).fetch_all()
    assert result.rows[0].get("n") > 0


def test_tx_profile_returns_real_operator_stats(db):
    session = db.session()
    result, profile = (
        session.tx().query_with("MATCH (a:E) RETURN count(a) AS c").profile()
    )
    assert result.rows[0].get("c") == 30
    assert profile.total_time_ms >= 0.0
    assert profile.operators, "a profile with no operators is not a profile"


def test_profile_honours_max_memory(db):
    """`profile()` used to accept the bound and ignore it, on every path.

    Asserted on the session arm because that is where the silent drop lived;
    the tx arm inherits the same plumbing.
    """
    session = db.session()
    with pytest.raises(uni_db.UniMemoryLimitExceededError):
        session.query_with(CYCLIC).max_memory(TINY).profile()


# --- metrics on both arms ---------------------------------------------------

LOCY_PROGRAM = """
CREATE RULE owns AS
  MATCH (a:E)-[:OWNS]->(b:E)
  YIELD KEY a, KEY b
QUERY owns RETURN a.uid AS f, b.uid AS t
"""


def test_both_arms_carry_metrics(db):
    """The first assertion on `.metrics` anywhere in the Python suite."""
    session = db.session()
    cypher = session.query("MATCH (a:E) RETURN count(a) AS c")
    locy = session.locy(LOCY_PROGRAM)

    assert type(cypher.metrics).__name__ == "QueryMetrics"
    assert type(locy.metrics).__name__ == "QueryMetrics", (
        "LocyResult must carry the same metrics type the Cypher arm does"
    )

    for result in (cypher, locy):
        assert result.metrics.total_time_ms > 0.0
        assert result.metrics.rows_returned >= 0


def test_locy_metrics_document_which_fields_are_unmeasured(db):
    """Locy does not go through the Cypher parse/plan/cache path.

    Those fields are therefore always zero here, and a zero that means "not
    measured" is indistinguishable from one that means "fast" -- so the stub
    says so, and this pins the claim rather than leaving it as prose.
    """
    metrics = db.session().locy(LOCY_PROGRAM).metrics
    assert metrics.parse_time_ms == 0.0
    assert metrics.plan_time_ms == 0.0
    assert metrics.plan_cache_hit is False
    # ... while the fields Locy does populate are real.
    assert metrics.total_time_ms > 0.0
    assert metrics.rows_returned > 0
