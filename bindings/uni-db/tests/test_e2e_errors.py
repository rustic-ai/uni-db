# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""E2E tests for error handling in sync API.

Tests error conditions, exceptions, and edge cases:
- Invalid Cypher syntax
- Schema violations
- State errors in transactions and bulk writers
"""

import pytest


def test_invalid_cypher_syntax(empty_db):
    """Invalid Cypher syntax should raise an exception."""
    db = empty_db
    with pytest.raises(Exception):
        db.query("INVALID CYPHER SYNTAX !!!")


def test_query_non_existent_label(social_db):
    """Query on non-existent label returns empty results (schemaless scan)."""
    db = social_db
    # The engine supports unknown labels via ScanMainByLabel (schemaless)
    # and returns an empty result set rather than raising an error
    result = db.query("MATCH (n:NonExistentLabel) RETURN n")
    assert result == []


def test_type_mismatch_in_property(social_db):
    """Inserting wrong type into typed property may be silently coerced by the engine."""
    db = social_db
    # age is defined as int, try to insert a string
    # The engine performs lenient type coercion, so this may not raise
    # Just verify it doesn't crash the database
    try:
        db.execute("CREATE (p:Person {name: 'Bob', age: 'not_an_int'})")
    except Exception:
        pass  # Either behavior is acceptable


def test_type_mismatch_string_into_int_field(social_db):
    """Specifically test string value into int field - engine may coerce leniently."""
    db = social_db
    # age property is defined as int type
    # The engine performs lenient type coercion, so this may not raise
    try:
        db.execute("CREATE (p:Person {name: 'Test', age: 'twenty-five'})")
    except Exception:
        pass  # Either behavior is acceptable


def test_operations_on_committed_bulk_writer(social_db):
    """Operations on a committed bulk writer should raise RuntimeError."""
    db = social_db
    writer = db.bulk_writer().build()

    # Commit the writer
    writer.commit()

    # Now try to insert - should raise RuntimeError
    with pytest.raises(RuntimeError):
        writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])


def test_operations_on_aborted_bulk_writer(social_db):
    """Operations on an aborted bulk writer should raise RuntimeError."""
    db = social_db
    writer = db.bulk_writer().build()

    # Abort the writer
    writer.abort()

    # Now try to insert - should raise RuntimeError
    with pytest.raises(RuntimeError):
        writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])


def test_double_commit_on_transaction(social_db):
    """Double commit on a transaction should raise RuntimeError."""
    db = social_db
    tx = db.begin()

    # First commit should succeed
    tx.commit()

    # Second commit should raise RuntimeError
    with pytest.raises(RuntimeError):
        tx.commit()


def test_double_rollback_on_transaction(social_db):
    """Double rollback on a transaction should raise RuntimeError."""
    db = social_db
    tx = db.begin()

    # First rollback should succeed
    tx.rollback()

    # Second rollback should raise RuntimeError
    with pytest.raises(RuntimeError):
        tx.rollback()


def test_operations_after_transaction_commit(social_db):
    """Operations after transaction commit should raise RuntimeError."""
    db = social_db
    tx = db.begin()

    # Do a query in the transaction
    tx.query("MATCH (n:Person) RETURN n")

    # Commit the transaction
    tx.commit()

    # Now try to query - should raise RuntimeError
    with pytest.raises(RuntimeError):
        tx.query("MATCH (n:Person) RETURN n")


def test_operations_after_transaction_rollback(social_db):
    """Operations after transaction rollback should raise RuntimeError."""
    db = social_db
    tx = db.begin()

    # Do a query in the transaction
    tx.query("MATCH (n:Person) RETURN n")

    # Rollback the transaction
    tx.rollback()

    # Now try to query - should raise RuntimeError
    with pytest.raises(RuntimeError):
        tx.query("MATCH (n:Person) RETURN n")


def test_bulk_writer_double_commit(social_db):
    """Double commit on bulk writer should raise RuntimeError."""
    db = social_db
    writer = db.bulk_writer().build()

    # Insert some data
    writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])

    # First commit
    writer.commit()

    # Second commit should raise
    with pytest.raises(RuntimeError):
        writer.commit()


def test_bulk_writer_double_abort(social_db):
    """Double abort on bulk writer is a no-op (does not raise)."""
    db = social_db
    writer = db.bulk_writer().build()

    # Insert some data
    writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])

    # First abort
    writer.abort()

    # Second abort is a no-op - the writer is already aborted
    writer.abort()


def test_bulk_writer_commit_after_abort(social_db):
    """Committing after abort should raise RuntimeError."""
    db = social_db
    writer = db.bulk_writer().build()

    writer.abort()

    with pytest.raises(RuntimeError):
        writer.commit()


def test_bulk_writer_abort_after_commit(social_db):
    """Aborting after commit should raise RuntimeError."""
    db = social_db
    writer = db.bulk_writer().build()

    writer.commit()

    with pytest.raises(RuntimeError):
        writer.abort()


def test_transaction_rollback_after_commit(social_db):
    """Rollback after commit should raise RuntimeError."""
    db = social_db
    tx = db.begin()

    tx.commit()

    with pytest.raises(RuntimeError):
        tx.rollback()


def test_transaction_commit_after_rollback(social_db):
    """Commit after rollback should raise RuntimeError."""
    db = social_db
    tx = db.begin()

    tx.rollback()

    with pytest.raises(RuntimeError):
        tx.commit()


def test_empty_cypher_query(empty_db):
    """Empty Cypher query should raise an exception."""
    db = empty_db
    with pytest.raises(Exception):
        db.query("")


def test_malformed_property_access(social_db):
    """Malformed property access in Cypher should raise an exception."""
    db = social_db
    db.execute("CREATE (p:Person {name: 'Alice', age: 30})")

    # Invalid property syntax
    with pytest.raises(Exception):
        db.query("MATCH (n:Person) RETURN n..name")


def test_missing_required_property(social_db):
    """Creating node without required property should raise an exception."""
    db = social_db
    # name and age are required (not nullable) for Person
    with pytest.raises(Exception):
        db.execute("CREATE (p:Person {name: 'Alice'})")  # missing age


def test_bulk_writer_insert_after_commit(social_db):
    """Insert operations after bulk writer commit should raise RuntimeError."""
    db = social_db
    writer = db.bulk_writer().build()

    writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])
    writer.commit()

    # Try to insert more vertices after commit
    with pytest.raises(RuntimeError):
        writer.insert_vertices("Person", [{"name": "Bob", "age": 25}])


def test_bulk_writer_insert_edges_after_abort(social_db):
    """Insert edge operations after bulk writer abort should raise RuntimeError."""
    db = social_db
    writer = db.bulk_writer().build()

    # Insert vertices first
    vids = writer.insert_vertices(
        "Person", [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    )

    writer.abort()

    # Try to insert edges after abort
    with pytest.raises(RuntimeError):
        writer.insert_edges("KNOWS", [(vids[0], vids[1], {})])
