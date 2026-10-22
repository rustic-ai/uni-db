# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Session.scratch(): a transaction whose writes are always discarded.

A scratch transaction is one way to run ABDUCE / EXPLAIN RULE against a
hypothetical change; an ASSUME body is the other, and both give the same
answer.
"""

import pytest

import uni_db

RULE = "CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b "
SEED = "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})"
HYPOTHETICAL = "MATCH (a:Node {name: 'A'}) CREATE (a)-[:EDGE]->(:Node {name: 'C'})"
ABDUCE_C = RULE + "ABDUCE NOT reachable WHERE b.name = 'C'"
ASSUME_ABDUCE = (
    RULE + "ASSUME { " + HYPOTHETICAL + " } "
    "THEN { ABDUCE NOT reachable WHERE b.name = 'C' }"
)
NAMES = "MATCH (n:Node) RETURN n.name AS name ORDER BY name"


def _abduce(result):
    abduces = [c for c in result.command_results if c.command_type == "abduce"]
    assert len(abduces) == 1, result.command_results
    return abduces[0].modifications


def _assume_abduce(result):
    """The modifications of the ABDUCE in the program's single ASSUME body."""
    assumes = [c for c in result.command_results if c.command_type == "assume"]
    assert len(assumes) == 1, result.command_results
    abduces = [c for c in assumes[0].results if c.command_type == "abduce"]
    assert len(abduces) == 1, assumes[0].results
    return abduces[0].modifications


def _validated(mods):
    return sorted(m["validated"] for m in mods)


# ── sync ──────────────────────────────────────────────────────────────────


def _sync_db():
    db = uni_db.Uni.temporary()
    session = db.session()
    tx = session.tx()
    tx.execute(SEED)
    tx.commit()
    return db, session


def test_scratch_reads_own_writes_and_discards_them():
    _db, session = _sync_db()
    tx = session.scratch()
    tx.execute(HYPOTHETICAL)
    inside = [r["name"] for r in tx.query(NAMES)]
    assert inside == ["A", "B", "C"]
    tx.rollback()
    assert [r["name"] for r in session.query(NAMES)] == ["A", "B"]


def test_scratch_commit_is_refused():
    _db, session = _sync_db()
    tx = session.scratch()
    tx.execute(HYPOTHETICAL)
    with pytest.raises(uni_db.UniReadOnlyError, match="scratch"):
        tx.commit()
    assert [r["name"] for r in session.query(NAMES)] == ["A", "B"]


def test_abduce_over_hypothetical_change_in_scratch():
    _db, session = _sync_db()
    tx = session.scratch()
    tx.execute(HYPOTHETICAL)
    mods = _abduce(tx.locy(ABDUCE_C))
    # The only way to stop A reaching C is to remove the hypothetical edge.
    assert any(m["validated"] for m in mods), mods
    tx.rollback()
    # Against the stored graph there is no C, so nothing to abduce.
    assert _abduce(session.locy(ABDUCE_C)) == []


def test_abduce_inside_assume_body_matches_scratch():
    _db, session = _sync_db()
    in_body = _assume_abduce(session.locy(ASSUME_ABDUCE))
    tx = session.scratch()
    tx.execute(HYPOTHETICAL)
    in_scratch = _abduce(tx.locy(ABDUCE_C))
    tx.rollback()
    # Removing the hypothetical edge is the one way to stop A reaching C.
    assert _validated(in_body) == _validated(in_scratch) == [True], (
        in_body,
        in_scratch,
    )
    assert [r["name"] for r in session.query(NAMES)] == ["A", "B"]


# ── async ─────────────────────────────────────────────────────────────────


async def _async_db():
    db = await uni_db.AsyncUni.temporary()
    session = db.session()
    tx = await session.tx()
    await tx.execute(SEED)
    await tx.commit()
    return db, session


async def _names(runner):
    return [r["name"] for r in await runner.query(NAMES)]


@pytest.mark.asyncio
async def test_async_scratch_reads_own_writes_and_discards_them():
    _db, session = await _async_db()
    tx = await session.scratch()
    await tx.execute(HYPOTHETICAL)
    assert await _names(tx) == ["A", "B", "C"]
    await tx.rollback()
    assert await _names(session) == ["A", "B"]


@pytest.mark.asyncio
async def test_async_scratch_commit_is_refused():
    _db, session = await _async_db()
    tx = await session.scratch()
    await tx.execute(HYPOTHETICAL)
    with pytest.raises(uni_db.UniReadOnlyError, match="scratch"):
        await tx.commit()
    assert await _names(session) == ["A", "B"]


@pytest.mark.asyncio
async def test_async_abduce_over_hypothetical_change_in_scratch():
    _db, session = await _async_db()
    tx = await session.scratch()
    await tx.execute(HYPOTHETICAL)
    mods = _abduce(await tx.locy(ABDUCE_C))
    assert any(m["validated"] for m in mods), mods
    await tx.rollback()
    assert _abduce(await session.locy(ABDUCE_C)) == []


@pytest.mark.asyncio
async def test_async_abduce_inside_assume_body_matches_scratch():
    _db, session = await _async_db()
    in_body = _assume_abduce(await session.locy(ASSUME_ABDUCE))
    tx = await session.scratch()
    await tx.execute(HYPOTHETICAL)
    in_scratch = _abduce(await tx.locy(ABDUCE_C))
    await tx.rollback()
    assert _validated(in_body) == _validated(in_scratch) == [True], (
        in_body,
        in_scratch,
    )
    assert await _names(session) == ["A", "B"]
