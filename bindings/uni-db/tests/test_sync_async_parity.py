# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Sync/async binding parity guard + functional tests.

The sync (`uni_db.Uni` & friends) and async (`uni_db.AsyncUni` & friends)
surfaces are meant to be 1:1 mirrors. `test_sync_async_method_parity` is the
regression guard against drift; the functional tests exercise the async
methods added to close known gaps: `AsyncUni.load_rhai_plugin`,
`AsyncTransaction.appender` / `appender_builder`
(`AsyncStreamingAppender` / `AsyncTxAppenderBuilder`), and the async
session-template classes.
"""

import pytest

import uni_db

# ---------------------------------------------------------------------------
# Parity guard
# ---------------------------------------------------------------------------

# Every `AsyncX` exported by the package must expose the same public method
# surface as its sync twin `X`.
#
# **Derived, not hand-maintained.** This was a literal list of 25 pairs, and it
# had fallen four behind the surface it guards: `AsyncCommitStream`,
# `AsyncRuleRegistry`, `AsyncXervo` and `AsyncBulkWriter` all existed, all had
# sync twins, and none was being compared. (All four matched when the list was
# replaced, so nothing was broken -- but nothing was watching either.)
#
# A hand-maintained list that drifts away from what it guards is the exact
# failure `scripts/ci/check_doc_symbols.py` was written about. Deriving the
# pairs means a new `Async*` class is covered the moment it is added, and the
# failure mode for a legitimate difference becomes "declare the exception
# below" rather than "silently never notice".


def _discover_pairs() -> list[tuple[str, str]]:
    """Every `(X, AsyncX)` pair reachable from the package."""
    names = {n for n in dir(uni_db) if not n.startswith("_")}
    return sorted(
        (n[len("Async") :], n)
        for n in names
        if n.startswith("Async") and n[len("Async") :] in names
    )


# Async classes with no sync twin, and why. An entry here is a claim that the
# asymmetry is intentional -- not a parking spot for a class nobody has looked
# at.
UNPAIRED_ASYNC: dict[str, str] = {}


SYNC_ASYNC_PAIRS = _discover_pairs()


def test_pair_discovery_is_not_vacuous():
    """The derived list must actually find the pairs.

    Without this, a rename that broke `_discover_pairs` would empty the list and
    every parity check below would pass by having nothing to check -- the
    parametrised tests would simply disappear from the run. The floor is the 25
    pairs the hand-written list carried; it should only ever grow.
    """
    assert len(SYNC_ASYNC_PAIRS) >= 25, (
        f"only {len(SYNC_ASYNC_PAIRS)} sync/async pairs discovered: {SYNC_ASYNC_PAIRS}"
    )


def test_every_async_class_is_paired_or_declared():
    """No `AsyncX` may quietly lack a sync twin."""
    unpaired = sorted(
        n
        for n in dir(uni_db)
        if not n.startswith("_")
        and n.startswith("Async")
        and n[len("Async") :] not in dir(uni_db)
        and n not in UNPAIRED_ASYNC
    )
    assert not unpaired, (
        f"async classes with no sync counterpart: {unpaired}.\n"
        "Add the sync class, or record the asymmetry in UNPAIRED_ASYNC with a "
        "reason."
    )


def _public_methods(cls):
    return {
        n for n in dir(cls) if not n.startswith("_") and callable(getattr(cls, n, None))
    }


@pytest.mark.parametrize("sync_name,async_name", SYNC_ASYNC_PAIRS)
def test_sync_async_method_parity(sync_name, async_name):
    sync_cls = getattr(uni_db, sync_name, None)
    async_cls = getattr(uni_db, async_name, None)
    assert sync_cls is not None, f"missing sync class uni_db.{sync_name}"
    assert async_cls is not None, f"missing async class uni_db.{async_name}"
    sync_methods = _public_methods(sync_cls)
    async_methods = _public_methods(async_cls)
    assert sync_methods == async_methods, (
        f"{sync_name} vs {async_name} method mismatch: "
        f"sync-only={sorted(sync_methods - async_methods)}, "
        f"async-only={sorted(async_methods - sync_methods)}"
    )


def test_async_query_builder_removed():
    """The dead, unreachable `AsyncQueryBuilder` was removed.

    Kept as a named regression even though
    `test_stub_drift.test_stub_has_no_phantom_classes` now generalises it. This
    one-off assertion existed for years while the same defect recurred five more
    times in the stub, precisely because nothing generalised it -- so the
    general check is the fix, and this line is the reminder of why it was
    needed.
    """
    assert not hasattr(uni_db, "AsyncQueryBuilder")


# ---------------------------------------------------------------------------
# Functional: AsyncUni.load_rhai_plugin
# ---------------------------------------------------------------------------

RHAI_SCRIPT = """
fn uni_manifest() {
    #{
        id: "ai.example.score",
        version: "0.1.0",
        determinism: "pure",
        scalar_fns: [
            #{ name: "score", args: ["float","float"], returns: "float" },
        ],
    }
}
fn score(x, y) { x * 0.7 + y * 0.3 }
"""


async def test_async_load_rhai_plugin_returns_metadata():
    db = await uni_db.AsyncUni.temporary()
    outcome = await db.load_rhai_plugin(RHAI_SCRIPT)
    assert outcome["plugin_id"] == "ai.example.score"
    assert outcome["version"] == "0.1.0"
    assert "ai.example.score.score" in outcome["scalars_registered"]
    assert outcome["aggregates_registered"] == []
    assert outcome["procedures_registered"] == []


async def test_async_load_rhai_plugin_explicit_grants():
    db = await uni_db.AsyncUni.temporary()
    outcome = await db.load_rhai_plugin(RHAI_SCRIPT, grants=["ScalarFn"])
    assert outcome["plugin_id"] == "ai.example.score"


async def test_async_load_rhai_plugin_rejects_bad_grant():
    db = await uni_db.AsyncUni.temporary()
    # Mirrors the sync behavior: unknown grants raise ValueError.
    with pytest.raises(ValueError):
        await db.load_rhai_plugin(RHAI_SCRIPT, grants=["NotARealCapability"])


async def test_async_load_rhai_plugin_rejects_bad_script():
    db = await uni_db.AsyncUni.temporary()
    with pytest.raises(Exception):
        await db.load_rhai_plugin("@@@ this is not rhai @@@")


# ---------------------------------------------------------------------------
# Functional: async streaming appender
# ---------------------------------------------------------------------------


async def test_async_appender_appends_and_persists():
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Person").property("name", "string").apply()
    session = db.session()
    tx = await session.tx()
    app = await tx.appender("Person")
    assert type(app).__name__ == "AsyncStreamingAppender"
    await app.append({"name": "Alice"})
    await app.append({"name": "Bob"})
    stats = await app.finish()
    assert stats.vertices_inserted == 2
    await tx.commit()

    results = await db.session().query("MATCH (n:Person) RETURN count(n) AS c")
    assert results[0]["c"] == 2


async def test_async_appender_builder_configures_and_builds():
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Item").property("sku", "string").apply()
    session = db.session()
    tx = await session.tx()
    builder = tx.appender_builder("Item")
    assert type(builder).__name__ == "AsyncTxAppenderBuilder"
    builder = builder.batch_size(64).max_buffer_size_bytes(1 << 20)
    app = await builder.build()
    assert type(app).__name__ == "AsyncStreamingAppender"
    await app.append({"sku": "A1"})
    stats = await app.finish()
    assert stats.vertices_inserted == 1
    await tx.commit()


# ---------------------------------------------------------------------------
# Functional: async session template
# ---------------------------------------------------------------------------


async def test_async_session_template_builds_async_session():
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Person").property("name", "string").apply()

    template = db.session_template().param("tenant", 1).build()
    assert type(template).__name__ == "AsyncSessionTemplate"

    session = template.create()
    assert type(session).__name__ == "AsyncSession"

    tx = await session.tx()
    await tx.execute("CREATE (n:Person {name: 'Zoe'})")
    await tx.commit()

    results = await session.query("MATCH (n:Person) RETURN n.name AS name")
    assert any(r["name"] == "Zoe" for r in results)


# ---------------------------------------------------------------------------
# Functional: cursor error-type parity
# ---------------------------------------------------------------------------

# A query that plans cleanly but fails during execution. The divisor comes from
# an UNWIND list so there is no constant for the optimizer to fold away, and
# `apply_int_arithmetic` raises rather than wrapping on integer div-by-zero.
#
# Note the cursor stream is single-shot: the whole plan executes inside the
# first `next_batch()` poll, so the error arrives on the *first* row rather
# than after the two successful divisions.
_FAILING_QUERY = "UNWIND [1, 2, 0] AS d RETURN 10 / d AS r"


def _sync_failing_cursor():
    db = uni_db.UniBuilder.temporary().build()
    return db.session().query_with(_FAILING_QUERY).cursor()


async def _async_failing_cursor():
    db = await uni_db.AsyncUniBuilder.temporary().build()
    return await db.session().query_with(_FAILING_QUERY).cursor()


def test_sync_cursor_raises_typed_error():
    """Baseline: the sync cursor surfaces the typed `UniQueryError`."""
    with pytest.raises(uni_db.UniQueryError):
        _sync_failing_cursor().fetch_one()


@pytest.mark.parametrize("method", ["fetch_one", "fetch_many", "aiter"])
async def test_async_cursor_raises_same_error_class_as_sync(method):
    """Async row-at-a-time reads must raise the same class the sync twin does.

    `next_row_async` used to return `Result<_, String>`, which erased the
    `UniError` variant; all three of its callers then had to invent a class and
    settled on bare `RuntimeError`. That made `_retry.RETRIABLE_EXCEPTIONS` --
    which matches by class -- unable to see a conflict raised through
    `async for`, so a retriable transaction failure became permanent.

    `fetch_all` is deliberately not covered here: it bypasses `next_row_async`
    for `collect_remaining`, so it was already correct and would mask the bug.
    """
    cursor = await _async_failing_cursor()

    with pytest.raises(uni_db.UniQueryError):
        if method == "fetch_one":
            await cursor.fetch_one()
        elif method == "fetch_many":
            await cursor.fetch_many(5)
        else:
            [row async for row in cursor]


async def test_async_cursor_still_stops_iteration_when_exhausted():
    """The `Ok(None)` arm must keep producing `StopAsyncIteration`.

    Widening the error type must not disturb exhaustion signalling, which is
    how `async for` terminates normally.
    """
    db = await uni_db.AsyncUniBuilder.temporary().build()
    cursor = await db.session().query_with("UNWIND [1, 2] AS d RETURN d AS r").cursor()

    rows = [row async for row in cursor]
    assert [r["r"] for r in rows] == [1, 2]
    assert await cursor.fetch_one() is None
