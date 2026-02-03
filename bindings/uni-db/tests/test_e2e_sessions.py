"""End-to-end tests for session functionality (sync API)."""

import pytest

import uni_db


@pytest.fixture
def social_db(tmp_path):
    """Create a database with social network schema."""
    db_path = tmp_path / "social_db"
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()

    # Create schema
    (
        db.schema()
        .label("User")
        .property("username", "string")
        .property("email", "string")
        .property("age", "int")
        .done()
        .label("Post")
        .property("title", "string")
        .property("content", "string")
        .property("likes", "int")
        .done()
        .edge_type("FOLLOWS", ["User"], ["User"])
        .done()
        .edge_type("POSTED", ["User"], ["Post"])
        .property_nullable("timestamp", "int")
        .done()
        .edge_type("LIKES", ["User"], ["Post"])
        .property_nullable("timestamp", "int")
        .done()
        .apply()
    )

    # Add test data
    db.execute("""
        CREATE (:User {username: 'alice', email: 'alice@example.com', age: 30})
    """)
    db.execute("""
        CREATE (:User {username: 'bob', email: 'bob@example.com', age: 25})
    """)
    db.execute("""
        CREATE (:User {username: 'charlie', email: 'charlie@example.com', age: 35})
    """)
    db.execute("""
        CREATE (:Post {title: 'Hello World', content: 'My first post!', likes: 5})
    """)
    db.execute("""
        CREATE (:Post {title: 'Graph Databases', content: 'They are awesome!', likes: 10})
    """)

    # Create relationships
    db.execute("""
        MATCH (a:User {username: 'alice'}), (b:User {username: 'bob'})
        CREATE (a)-[:FOLLOWS]->(b)
    """)
    db.execute("""
        MATCH (a:User {username: 'alice'}), (p:Post {title: 'Hello World'})
        CREATE (a)-[:POSTED {timestamp: 1234567890}]->(p)
    """)
    db.execute("""
        MATCH (b:User {username: 'bob'}), (p:Post {title: 'Hello World'})
        CREATE (b)-[:LIKES {timestamp: 1234567900}]->(p)
    """)

    db.flush()
    yield db


def test_session_with_single_variable(social_db):
    """Test creating a session with a single variable."""
    db = social_db

    # Create session with a variable
    builder = db.session()
    builder.set("username", "alice")
    session = builder.build()

    # Verify we can get the variable back
    value = session.get("username")
    assert value == "alice"


def test_session_with_multiple_variables(social_db):
    """Test creating a session with multiple variables."""
    db = social_db

    # Create session with multiple variables
    builder = db.session()
    builder.set("username", "bob")
    builder.set("min_age", 20)
    builder.set("max_age", 30)
    builder.set("is_active", True)
    session = builder.build()

    # Verify all variables
    assert session.get("username") == "bob"
    assert session.get("min_age") == 20
    assert session.get("max_age") == 30
    assert session.get("is_active") is True


def test_session_get_nonexistent_returns_none(social_db):
    """Test that getting a non-existent variable returns None."""
    db = social_db

    builder = db.session()
    builder.set("existing_key", "value")
    session = builder.build()

    # Get existing key
    assert session.get("existing_key") == "value"

    # Get non-existent key
    assert session.get("nonexistent_key") is None


def test_session_query(social_db):
    """Test executing queries within a session."""
    db = social_db

    # Create session
    builder = db.session()
    builder.set("target_username", "alice")
    session = builder.build()

    # Execute query using session variable
    results = session.query("""
        MATCH (u:User {username: $session.target_username})
        RETURN u.username AS username, u.email AS email, u.age AS age
    """)

    assert len(results) == 1
    assert results[0]["username"] == "alice"
    assert results[0]["email"] == "alice@example.com"
    assert results[0]["age"] == 30


def test_session_query_with_params(social_db):
    """Test session query with additional parameters."""
    db = social_db

    # Create session with one variable
    builder = db.session()
    builder.set("min_likes", 5)
    session = builder.build()

    # Query with additional params
    results = session.query(
        """
        MATCH (p:Post)
        WHERE p.likes >= $session.min_likes AND p.likes <= $max_likes
        RETURN p.title AS title, p.likes AS likes
        ORDER BY likes
        """,
        params={"max_likes": 10},
    )

    assert len(results) == 2
    assert results[0]["title"] == "Hello World"
    assert results[0]["likes"] == 5
    assert results[1]["title"] == "Graph Databases"
    assert results[1]["likes"] == 10


def test_session_execute(social_db):
    """Test executing write operations within a session."""
    db = social_db

    # Create session
    builder = db.session()
    builder.set("new_username", "diana")
    builder.set("new_email", "diana@example.com")
    builder.set("new_age", 28)
    session = builder.build()

    # Execute create operation
    count = session.execute("""
        CREATE (:User {username: $session.new_username, email: $session.new_email, age: $session.new_age})
    """)

    # execute returns number of affected rows/nodes
    assert isinstance(count, int)

    # Verify the user was created
    results = db.query(
        "MATCH (u:User {username: 'diana'}) RETURN u.username AS username"
    )
    assert len(results) == 1
    assert results[0]["username"] == "diana"


def test_session_variables_persist_across_queries(social_db):
    """Test that session variables persist across multiple queries."""
    db = social_db

    # Create session
    builder = db.session()
    builder.set("user1", "alice")
    builder.set("user2", "bob")
    session = builder.build()

    # First query
    results1 = session.query("""
        MATCH (u:User {username: $session.user1})
        RETURN u.username AS username
    """)
    assert results1[0]["username"] == "alice"

    # Second query using different variable
    results2 = session.query("""
        MATCH (u:User {username: $session.user2})
        RETURN u.username AS username
    """)
    assert results2[0]["username"] == "bob"

    # Third query using both variables
    results3 = session.query("""
        MATCH (u1:User {username: $session.user1})-[:FOLLOWS]->(u2:User {username: $session.user2})
        RETURN u1.username AS follower, u2.username AS followed
    """)
    assert len(results3) == 1
    assert results3[0]["follower"] == "alice"
    assert results3[0]["followed"] == "bob"


def test_session_with_complex_types(social_db):
    """Test session variables with complex types."""
    db = social_db

    # Create session with various types
    builder = db.session()
    builder.set("string_val", "test")
    builder.set("int_val", 42)
    builder.set("float_val", 3.14)
    builder.set("bool_val", True)
    builder.set("list_val", [1, 2, 3])
    builder.set("dict_val", {"key": "value", "nested": {"deep": "data"}})
    session = builder.build()

    # Verify all types
    assert session.get("string_val") == "test"
    assert session.get("int_val") == 42
    assert session.get("float_val") == 3.14
    assert session.get("bool_val") is True
    assert session.get("list_val") == [1, 2, 3]
    assert session.get("dict_val") == {"key": "value", "nested": {"deep": "data"}}


def test_session_execute_update(social_db):
    """Test executing update operations within a session."""
    db = social_db

    # Create session
    builder = db.session()
    builder.set("target_user", "charlie")
    builder.set("new_age", 36)
    session = builder.build()

    # Execute update
    count = session.execute("""
        MATCH (u:User {username: $session.target_user})
        SET u.age = $session.new_age
    """)

    assert isinstance(count, int)

    # Verify update
    results = db.query("MATCH (u:User {username: 'charlie'}) RETURN u.age AS age")
    assert results[0]["age"] == 36


def test_session_execute_delete(social_db):
    """Test executing delete operations within a session."""
    db = social_db

    # Verify initial state
    initial = db.query(
        "MATCH (p:Post {title: 'Graph Databases'}) RETURN count(p) AS count"
    )
    assert initial[0]["count"] == 1

    # Create session
    builder = db.session()
    builder.set("post_title", "Graph Databases")
    session = builder.build()

    # Execute delete
    count = session.execute("""
        MATCH (p:Post {title: $session.post_title})
        DELETE p
    """)

    assert isinstance(count, int)

    # Verify deletion
    results = db.query(
        "MATCH (p:Post {title: 'Graph Databases'}) RETURN count(p) AS count"
    )
    assert results[0]["count"] == 0


def test_multiple_independent_sessions(social_db):
    """Test that multiple sessions are independent."""
    db = social_db

    # Create first session
    builder1 = db.session()
    builder1.set("username", "alice")
    session1 = builder1.build()

    # Create second session with different values
    builder2 = db.session()
    builder2.set("username", "bob")
    session2 = builder2.build()

    # Verify sessions are independent
    assert session1.get("username") == "alice"
    assert session2.get("username") == "bob"

    # Query using both sessions
    results1 = session1.query(
        "MATCH (u:User {username: $session.username}) RETURN u.username AS username"
    )
    results2 = session2.query(
        "MATCH (u:User {username: $session.username}) RETURN u.username AS username"
    )

    assert results1[0]["username"] == "alice"
    assert results2[0]["username"] == "bob"


def test_session_with_aggregations(social_db):
    """Test session queries with aggregation functions."""
    db = social_db

    # Create session
    builder = db.session()
    builder.set("min_age", 25)
    session = builder.build()

    # Query with aggregation
    results = session.query("""
        MATCH (u:User)
        WHERE u.age >= $session.min_age
        RETURN count(u) AS user_count, avg(u.age) AS avg_age
    """)

    assert len(results) == 1
    assert results[0]["user_count"] == 3
    assert isinstance(results[0]["avg_age"], (int, float))


def test_session_builder_chaining(social_db):
    """Test that session builder methods can be chained."""
    db = social_db

    # Build session with chained calls (if API supports it)
    builder = db.session()
    builder.set("var1", "value1")
    builder.set("var2", "value2")
    builder.set("var3", "value3")
    session = builder.build()

    # Verify all variables were set
    assert session.get("var1") == "value1"
    assert session.get("var2") == "value2"
    assert session.get("var3") == "value3"


def test_session_with_relationship_queries(social_db):
    """Test session queries involving relationships."""
    db = social_db

    # Create session
    builder = db.session()
    builder.set("follower", "alice")
    session = builder.build()

    # Query relationships
    results = session.query("""
        MATCH (u:User {username: $session.follower})-[:FOLLOWS]->(followed:User)
        RETURN followed.username AS followed_username
    """)

    assert len(results) == 1
    assert results[0]["followed_username"] == "bob"


def test_session_query_returns_empty_list(social_db):
    """Test that session query returns empty list when no matches."""
    db = social_db

    builder = db.session()
    builder.set("username", "nonexistent_user")
    session = builder.build()

    results = session.query("""
        MATCH (u:User {username: $session.username})
        RETURN u.username AS username
    """)

    assert results == []
    assert isinstance(results, list)
