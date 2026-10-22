# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Bulk-load commit outcome: the load id, the applied check, and the typed errors."""

import uuid

import pytest

import uni_db


def _schema(db):
    db.schema().label("Person").property("name", "string").done().apply()


def _rows(n):
    return [{"name": f"p{i}"} for i in range(n)]


def test_load_id_and_applied_check_survive_reopen(tmp_path):
    db = uni_db.UniBuilder.open(str(tmp_path)).build()
    _schema(db)
    tx = db.session().tx()
    writer = tx.bulk_writer().batch_size(2).build()
    load_id = writer.load_id
    uuid.UUID(load_id)
    writer.insert_vertices("Person", _rows(4))
    assert db.bulk_load_applied(load_id) is False, "staged, before the commit point"
    stats = writer.commit()
    tx.commit()
    assert stats.load_id == load_id
    assert stats.warnings == []
    assert writer.load_id == load_id, "readable after commit"
    assert db.bulk_load_applied(load_id) is True

    tx = db.session().tx()
    aborted = tx.bulk_writer().build()
    aborted.insert_vertices("Person", _rows(2))
    aborted.abort()
    tx.commit()
    assert aborted.load_id != load_id
    assert db.bulk_load_applied(aborted.load_id) is False
    del db

    db = uni_db.UniBuilder.open(str(tmp_path)).build()
    assert db.bulk_load_applied(load_id) is True, "from the receipt, after reopen"
    assert db.bulk_load_applied(aborted.load_id) is False
    assert db.bulk_load_applied(str(uuid.uuid4())) is False
    with pytest.raises(uni_db.UniInvalidArgumentError):
        db.bulk_load_applied("../catalog/schema")
    rows = db.session().query("MATCH (p:Person) RETURN count(p) AS c")
    assert rows[0]["c"] == 4


@pytest.mark.asyncio
async def test_async_load_id_and_applied_check():
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Person").property("name", "string").done().apply()
    tx = await db.session().tx()
    writer = await tx.bulk_writer().build()
    load_id = writer.load_id
    await writer.insert_vertices("Person", _rows(3))
    assert await db.bulk_load_applied(load_id) is False
    stats = await writer.commit()
    await tx.commit()
    assert stats.load_id == load_id
    assert stats.warnings == []
    assert writer.load_id == load_id
    assert await db.bulk_load_applied(load_id) is True
    with pytest.raises(uni_db.UniInvalidArgumentError):
        await db.bulk_load_applied("not-a-load-id")


@pytest.mark.parametrize(
    "exc",
    [uni_db.UniBulkCommitIncompleteError, uni_db.UniBulkCommitOutcomeUnknownError],
)
def test_commit_outcome_exceptions_are_distinct_uni_errors(exc):
    # Neither may be caught as the generic "not applied" failure by accident:
    # both are UniError subclasses of their own, carrying the load id.
    assert issubclass(exc, uni_db.UniError)
    assert exc is not uni_db.UniInternalError
    assert not issubclass(exc, uni_db.UniInternalError)
    err = exc("committed")
    err.load_id = "id"
    assert err.load_id == "id"


def test_fork_session_answers_for_its_own_loads(tmp_path):
    # A fork's load keeps its marker and receipt in the fork's namespace: the
    # fork's session reports it applied, the primary does not know it.
    db = uni_db.UniBuilder.open(str(tmp_path)).build()
    _schema(db)
    db.flush()
    fork = db.session().fork("bulk_fork").build()
    tx = fork.tx()
    writer = tx.bulk_writer().build()
    load_id = writer.load_id
    writer.insert_vertices("Person", _rows(3))
    assert fork.bulk_load_applied(load_id) is False, "before the commit point"
    writer.commit()
    tx.commit()
    assert fork.bulk_load_applied(load_id) is True
    assert db.bulk_load_applied(load_id) is False
    assert db.session().bulk_load_applied(load_id) is False
    with pytest.raises(uni_db.UniInvalidArgumentError):
        fork.bulk_load_applied("not-a-load-id")


@pytest.mark.asyncio
async def test_async_fork_session_answers_for_its_own_loads():
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Person").property("name", "string").done().apply()
    await db.flush()
    fork = await db.session().fork("bulk_fork").build()
    tx = await fork.tx()
    writer = await tx.bulk_writer().build()
    load_id = writer.load_id
    await writer.insert_vertices("Person", _rows(2))
    await writer.commit()
    await tx.commit()
    assert await fork.bulk_load_applied(load_id) is True
    assert await db.bulk_load_applied(load_id) is False
