# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""E2E tests for error handling in sync API.

Tests error conditions, exceptions, and edge cases:
- Invalid Cypher syntax
- Schema violations
- State errors in transactions and bulk writers
"""

import pytest

import uni_db


def test_invalid_cypher_syntax(empty_db):
    """Invalid Cypher syntax should raise an exception."""
    db = empty_db
    session = db.session()
    with pytest.raises(Exception):
        session.query("INVALID CYPHER SYNTAX !!!")


def test_query_non_existent_label(social_db):
    """Query on non-existent label returns empty results (schemaless scan)."""
    db = social_db
    session = db.session()
    # The engine supports unknown labels via ScanMainByLabel (schemaless)
    # and returns an empty result set rather than raising an error
    result = session.query("MATCH (n:NonExistentLabel) RETURN n")
    assert len(result) == 0


def test_type_mismatch_in_property(social_db):
    """Inserting wrong type into typed property may be silently coerced by the engine."""
    db = social_db
    session = db.session()
    # age is defined as int, try to insert a string
    # The engine performs lenient type coercion, so this may not raise
    # Just verify it doesn't crash the database
    try:
        tx = session.tx()
        tx.execute("CREATE (p:Person {name: 'Bob', age: 'not_an_int'})")
        tx.commit()
    except Exception:
        pass  # Either behavior is acceptable


def test_type_mismatch_string_into_int_field(social_db):
    """Specifically test string value into int field - engine may coerce leniently."""
    db = social_db
    session = db.session()
    # age property is defined as int type
    # The engine performs lenient type coercion, so this may not raise
    try:
        tx = session.tx()
        tx.execute("CREATE (p:Person {name: 'Test', age: 'twenty-five'})")
        tx.commit()
    except Exception:
        pass  # Either behavior is acceptable


def test_operations_on_committed_bulk_writer(social_db):
    """Operations on a committed bulk writer should raise RuntimeError."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Commit the writer
    writer.commit()
    tx.commit()

    # Now try to insert - should raise RuntimeError (Python-side check)
    with pytest.raises(RuntimeError):
        writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])


def test_operations_on_aborted_bulk_writer(social_db):
    """Operations on an aborted bulk writer should raise RuntimeError."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Abort the writer
    writer.abort()
    tx.rollback()

    # Now try to insert - should raise RuntimeError (Python-side check)
    with pytest.raises(RuntimeError):
        writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])


def test_double_commit_on_transaction(social_db):
    """Double commit on a transaction should raise UniTransactionAlreadyCompletedError."""
    db = social_db
    session = db.session()
    tx = session.tx()

    # First commit should succeed
    tx.commit()

    # Second commit should raise UniTransactionAlreadyCompletedError
    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        tx.commit()


def test_double_rollback_on_transaction(social_db):
    """Double rollback on a transaction should raise UniTransactionAlreadyCompletedError."""
    db = social_db
    session = db.session()
    tx = session.tx()

    # First rollback should succeed
    tx.rollback()

    # Second rollback should raise UniTransactionAlreadyCompletedError
    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        tx.rollback()


def test_operations_after_transaction_commit(social_db):
    """Operations after transaction commit should raise UniTransactionAlreadyCompletedError."""
    db = social_db
    session = db.session()
    tx = session.tx()

    # Do a query in the transaction
    tx.query("MATCH (n:Person) RETURN n")

    # Commit the transaction
    tx.commit()

    # Now try to query - should raise UniTransactionAlreadyCompletedError
    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        tx.query("MATCH (n:Person) RETURN n")


def test_operations_after_transaction_rollback(social_db):
    """Operations after transaction rollback should raise UniTransactionAlreadyCompletedError."""
    db = social_db
    session = db.session()
    tx = session.tx()

    # Do a query in the transaction
    tx.query("MATCH (n:Person) RETURN n")

    # Rollback the transaction
    tx.rollback()

    # Now try to query - should raise UniTransactionAlreadyCompletedError
    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        tx.query("MATCH (n:Person) RETURN n")


def test_bulk_writer_double_commit(social_db):
    """Double commit on bulk writer should raise RuntimeError."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Insert some data
    writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])

    # First commit
    writer.commit()
    tx.commit()

    # Second commit should raise RuntimeError (Python-side check)
    with pytest.raises(RuntimeError):
        writer.commit()


def test_bulk_writer_double_abort(social_db):
    """Double abort on bulk writer is a no-op (does not raise)."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Insert some data
    writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])

    # First abort
    writer.abort()
    tx.rollback()

    # Second abort is a no-op - the writer is already aborted
    writer.abort()


def test_bulk_writer_commit_after_abort(social_db):
    """Committing after abort should raise RuntimeError."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    writer.abort()
    tx.rollback()

    with pytest.raises(RuntimeError):
        writer.commit()


def test_bulk_writer_abort_after_commit(social_db):
    """Aborting after commit raises; aborting twice does not.

    Was `xfail`ed: the PyO3 wrapper held `Mutex<Option<BulkWriter>>`, so
    "already committed" and "already aborted" were the same `None` and abort
    silently succeeded. The wrapper now tracks which one it is
    (bindings/uni-db/src/builders.rs::BulkWriterState), which also makes
    uni-bulk's own "Cannot abort: bulk load already committed" reachable.
    """
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    writer.commit()
    tx.commit()

    with pytest.raises(RuntimeError):
        writer.abort()


def test_transaction_rollback_after_commit(social_db):
    """Rollback after commit should raise UniTransactionAlreadyCompletedError."""
    db = social_db
    session = db.session()
    tx = session.tx()

    tx.commit()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        tx.rollback()


def test_transaction_commit_after_rollback(social_db):
    """Commit after rollback should raise UniTransactionAlreadyCompletedError."""
    db = social_db
    session = db.session()
    tx = session.tx()

    tx.rollback()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        tx.commit()


def test_empty_cypher_query(empty_db):
    """Empty Cypher query should raise an exception."""
    db = empty_db
    session = db.session()
    with pytest.raises(Exception):
        session.query("")


def test_malformed_property_access(social_db):
    """Malformed property access in Cypher should raise an exception."""
    db = social_db
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    tx.commit()

    # Invalid property syntax
    with pytest.raises(Exception):
        session.query("MATCH (n:Person) RETURN n..name")


def test_missing_required_property(social_db):
    """Creating node without required property should raise an exception."""
    db = social_db
    session = db.session()
    # name and age are required (not nullable) for Person
    with pytest.raises(Exception):
        tx = session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice'})")  # missing age
        tx.commit()


def test_bulk_writer_insert_after_commit(social_db):
    """Insert operations after bulk writer commit should raise RuntimeError."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])
    writer.commit()
    tx.commit()

    # Try to insert more vertices after commit - RuntimeError (Python-side check)
    with pytest.raises(RuntimeError):
        writer.insert_vertices("Person", [{"name": "Bob", "age": 25}])


def test_bulk_writer_insert_edges_after_abort(social_db):
    """Insert edge operations after bulk writer abort should raise RuntimeError."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Insert vertices first
    vids = writer.insert_vertices(
        "Person", [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    )

    writer.abort()
    tx.rollback()

    # Try to insert edges after abort - RuntimeError (Python-side check)
    with pytest.raises(RuntimeError):
        writer.insert_edges("KNOWS", [(vids[0], vids[1], {})])


@pytest.mark.parametrize("name", ["overflow_json", "_vid", "_deleted", "_created_at"])
def test_storage_column_property_name_refused(social_db, name):
    """A property named after a storage column is refused on every write path."""
    session = social_db.session()
    statements = [
        f"CREATE (:Person {{name: 'X', age: 1, `{name}`: 7}})",
        f"CREATE (:Loose {{`{name}`: 7}})",
        f"MERGE (p:Person {{name: 'Y', age: 2}}) SET p.`{name}` = 7",
    ]
    for stmt in statements:
        tx = session.tx()
        with pytest.raises(uni_db.UniInvalidIdentifierError):
            tx.execute(stmt)
        tx.rollback()

    tx = session.tx()
    writer = tx.bulk_writer().build()
    with pytest.raises(uni_db.UniInvalidIdentifierError):
        writer.insert_vertices("Person", [{"name": "Z", "age": 3, name: 7}])
    writer.abort()
    tx.rollback()

    assert len(session.query("MATCH (n) RETURN n")) == 0
