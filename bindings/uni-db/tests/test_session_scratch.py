# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Session.scratch(): a transaction whose writes are always discarded.

Also the replacement for ABDUCE / EXPLAIN RULE inside an ASSUME body, which is
now a compile error: apply the hypothetical change in a scratch transaction and
run the command there.
"""

import pytest

import uni_db

RULE = "CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b "
SEED = "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})"
HYPOTHETICAL = "MATCH (a:Node {name: 'A'}) CREATE (a)-[:EDGE]->(:Node {name: 'C'})"
ABDUCE_C = RULE + "ABDUCE NOT reachable WHERE b.name = 'C'"
ASSUME_ABDUCE = (
    RULE + "ASSUME { CREATE (:Node {name: 'X'}) } "
    "THEN { ABDUCE NOT reachable WHERE a.name = 'A' }"
)
NAMES = "MATCH (n:Node) RETURN n.name AS name ORDER BY name"


def _abduce(result):
    abduces = [c for c in result.command_results if c.command_type == "abduce"]
    assert len(abduces) == 1, result.command_results
    return abduces[0].modifications


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


def test_abduce_inside_assume_body_is_a_compile_error():
    _db, session = _sync_db()
    with pytest.raises(uni_db.UniError, match="not allowed inside an ASSUME"):
        session.locy(ASSUME_ABDUCE)


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
async def test_async_abduce_inside_assume_body_is_a_compile_error():
    _db, session = await _async_db()
    with pytest.raises(uni_db.UniError, match="not allowed inside an ASSUME"):
        await session.locy(ASSUME_ABDUCE)
