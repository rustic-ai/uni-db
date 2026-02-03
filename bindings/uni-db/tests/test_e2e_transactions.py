# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""E2E tests for synchronous transaction API.

Tests cover basic transaction lifecycle, commit/rollback semantics,
isolation, error handling, and edge cases using the social_db fixture.
"""

import pytest


def test_begin_and_commit(social_db):
    """Test basic transaction lifecycle: begin, query, commit, verify persistence."""
    # Begin transaction
    tx = social_db.begin()

    # Insert data within transaction
    tx.query("CREATE (p:Person {name: 'Alice', age: 30})")

    # Commit transaction
    tx.commit()

    # Verify data persists after commit
    results = social_db.query(
        "MATCH (p:Person {name: 'Alice'}) RETURN p.name AS name, p.age AS age"
    )
    assert len(results) == 1
    assert results[0]["name"] == "Alice"
    assert results[0]["age"] == 30


def test_begin_and_rollback(social_db):
    """Test rollback: data should be reverted after rollback."""
    # Insert initial data
    social_db.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    social_db.flush()

    # Begin transaction
    tx = social_db.begin()

    # Insert new data within transaction
    tx.query("CREATE (p:Person {name: 'Charlie', age: 35})")

    # Rollback transaction
    tx.rollback()

    # Verify rollback: Charlie should not exist
    results = social_db.query(
        "MATCH (p:Person {name: 'Charlie'}) RETURN p.name AS name"
    )
    assert len(results) == 0

    # Verify Bob still exists (committed before transaction)
    results = social_db.query("MATCH (p:Person {name: 'Bob'}) RETURN p.name AS name")
    assert len(results) == 1


def test_transaction_query_with_parameters(social_db):
    """Test transaction query with parameters."""
    tx = social_db.begin()

    # Use parameters in transaction query
    params = {"name": "Diana", "age": 28, "email": "diana@example.com"}
    tx.query("CREATE (p:Person {name: $name, age: $age, email: $email})", params)

    tx.commit()

    # Verify data was created with correct parameters
    results = social_db.query(
        "MATCH (p:Person {name: 'Diana'}) RETURN p.name AS name, p.age AS age, p.email AS email"
    )
    assert len(results) == 1
    assert results[0]["name"] == "Diana"
    assert results[0]["age"] == 28
    assert results[0]["email"] == "diana@example.com"


def test_multiple_operations_in_one_transaction(social_db):
    """Test multiple operations within a single transaction."""
    tx = social_db.begin()

    # Multiple creates
    tx.query("CREATE (p:Person {name: 'Alice', age: 30})")
    tx.query("CREATE (p:Person {name: 'Bob', age: 25})")
    tx.query("CREATE (c:Company {name: 'TechCorp', founded: 2010})")

    # Create relationship
    tx.query(
        "MATCH (a:Person {name: 'Alice'}), (t:Company {name: 'TechCorp'}) "
        "CREATE (a)-[:WORKS_AT {role: 'Engineer'}]->(t)"
    )

    tx.commit()

    # Verify all data was committed
    person_count = social_db.query("MATCH (p:Person) RETURN count(p) AS count")
    assert person_count[0]["count"] == 2

    company_count = social_db.query("MATCH (c:Company) RETURN count(c) AS count")
    assert company_count[0]["count"] == 1

    works_at = social_db.query(
        "MATCH (p:Person {name: 'Alice'})-[r:WORKS_AT]->(c:Company) "
        "RETURN r.role AS role"
    )
    assert len(works_at) == 1
    assert works_at[0]["role"] == "Engineer"


def test_transaction_isolation_changes_visible_inside_tx(social_db):
    """Test transaction isolation: changes visible inside transaction."""
    tx = social_db.begin()

    # Create person
    tx.query("CREATE (p:Person {name: 'Eve', age: 32})")

    # Query within same transaction - should see the change
    results = tx.query(
        "MATCH (p:Person {name: 'Eve'}) RETURN p.name AS name, p.age AS age"
    )
    assert len(results) == 1
    assert results[0]["name"] == "Eve"
    assert results[0]["age"] == 32

    # Rollback for cleanup
    tx.rollback()


def test_transaction_commit_makes_changes_visible_outside(social_db):
    """Test that committed transaction changes are visible outside."""
    tx = social_db.begin()

    # Create data within transaction
    tx.query("CREATE (p:Person {name: 'Frank', age: 40})")

    # Commit transaction
    tx.commit()

    # After commit, data should be visible outside
    results = social_db.query("MATCH (p:Person {name: 'Frank'}) RETURN p.name AS name")
    assert len(results) == 1
    assert results[0]["name"] == "Frank"


def test_double_commit_raises_error(social_db):
    """Test that double commit raises RuntimeError."""
    tx = social_db.begin()
    tx.query("CREATE (p:Person {name: 'Grace', age: 29})")

    # First commit should succeed
    tx.commit()

    # Second commit should raise RuntimeError
    with pytest.raises(RuntimeError, match="Transaction already completed"):
        tx.commit()


def test_double_rollback_raises_error(social_db):
    """Test that double rollback raises RuntimeError."""
    tx = social_db.begin()
    tx.query("CREATE (p:Person {name: 'Henry', age: 33})")

    # First rollback should succeed
    tx.rollback()

    # Second rollback should raise RuntimeError
    with pytest.raises(RuntimeError, match="Transaction already completed"):
        tx.rollback()


def test_operations_after_commit_raise_error(social_db):
    """Test that operations after commit raise RuntimeError."""
    tx = social_db.begin()
    tx.query("CREATE (p:Person {name: 'Iris', age: 27})")

    # Commit transaction
    tx.commit()

    # Attempting to query after commit should raise RuntimeError
    with pytest.raises(RuntimeError, match="Transaction already completed"):
        tx.query("CREATE (p:Person {name: 'Jack', age: 31})")

    # Attempting to rollback after commit should raise RuntimeError
    with pytest.raises(RuntimeError, match="Transaction already completed"):
        tx.rollback()


def test_operations_after_rollback_raise_error(social_db):
    """Test that operations after rollback raise RuntimeError."""
    tx = social_db.begin()
    tx.query("CREATE (p:Person {name: 'Kate', age: 26})")

    # Rollback transaction
    tx.rollback()

    # Attempting to query after rollback should raise RuntimeError
    with pytest.raises(RuntimeError, match="Transaction already completed"):
        tx.query("CREATE (p:Person {name: 'Leo', age: 34})")

    # Attempting to commit after rollback should raise RuntimeError
    with pytest.raises(RuntimeError, match="Transaction already completed"):
        tx.commit()


def test_rollback_after_partial_writes(social_db):
    """Test rollback reverts all partial writes within a transaction."""
    # Insert initial data
    social_db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    social_db.flush()

    # Begin transaction
    tx = social_db.begin()

    # Multiple partial writes (avoid cross-context MATCH for edges)
    tx.query("CREATE (p:Person {name: 'Bob', age: 25})")
    tx.query("CREATE (c:Company {name: 'StartupInc', founded: 2020})")

    # Rollback all writes
    tx.rollback()

    # Verify Bob was not created
    bob_results = social_db.query(
        "MATCH (p:Person {name: 'Bob'}) RETURN p.name AS name"
    )
    assert len(bob_results) == 0

    # Verify StartupInc was not created
    company_results = social_db.query(
        "MATCH (c:Company {name: 'StartupInc'}) RETURN c.name AS name"
    )
    assert len(company_results) == 0

    # Verify Alice still exists (committed before transaction)
    alice_results = social_db.query(
        "MATCH (p:Person {name: 'Alice'}) RETURN p.name AS name"
    )
    assert len(alice_results) == 1
