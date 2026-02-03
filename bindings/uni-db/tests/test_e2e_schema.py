# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
"""Comprehensive sync schema management E2E tests.

Tests all schema operations including:
- Direct schema creation methods (create_label, create_edge_type, add_property)
- Schema builder API with fluent interface
- Property types (string, int, float, bool, vector, list)
- Nullable vs non-nullable properties
- Schema introspection (list_labels, get_label_info, get_schema)
- Schema persistence (save_schema, load_schema)
- End-to-end data type verification
"""

import pytest

import uni_db

# =============================================================================
# Direct Schema Creation Methods
# =============================================================================


def test_create_label_direct(empty_db):
    """Test creating labels via direct method."""
    db = empty_db

    label_id = db.create_label("Person")
    assert isinstance(label_id, int)
    assert label_id >= 0

    assert db.label_exists("Person")
    assert "Person" in db.list_labels()


def test_create_edge_type_direct(empty_db):
    """Test creating edge types via direct method."""
    db = empty_db

    # Create labels first
    db.create_label("Person")
    db.create_label("Company")

    # Create edge type
    edge_id = db.create_edge_type("WORKS_AT", ["Person"], ["Company"])
    assert isinstance(edge_id, int)
    assert edge_id >= 0

    assert db.edge_type_exists("WORKS_AT")
    assert "WORKS_AT" in db.list_edge_types()


def test_add_properties_direct(empty_db):
    """Test adding properties via direct method (string, int, float, bool)."""
    db = empty_db

    db.create_label("Person")
    db.add_property("Person", "name", "string", False)
    db.add_property("Person", "age", "int", False)
    db.add_property("Person", "salary", "float", False)
    db.add_property("Person", "active", "bool", False)

    # Verify via schema introspection
    info = db.get_label_info("Person")
    assert info is not None
    assert len(info.properties) == 4

    prop_names = {p.name for p in info.properties}
    assert prop_names == {"name", "age", "salary", "active"}


def test_add_nullable_properties_direct(empty_db):
    """Test adding nullable properties via direct method."""
    db = empty_db

    db.create_label("Person")
    db.add_property("Person", "name", "string", False)
    db.add_property("Person", "email", "string", True)
    db.add_property("Person", "phone", "string", True)

    info = db.get_label_info("Person")
    assert info is not None

    # Find nullable properties
    nullable_props = {p.name for p in info.properties if p.nullable}
    assert nullable_props == {"email", "phone"}

    # Verify name is not nullable
    name_prop = next(p for p in info.properties if p.name == "name")
    assert not name_prop.nullable


def test_add_vector_property_direct(empty_db):
    """Test adding vector properties via direct method."""
    db = empty_db

    db.create_label("Document")
    db.add_property("Document", "title", "string", False)
    db.add_property("Document", "embedding", "vector:128", False)

    info = db.get_label_info("Document")
    assert info is not None

    # Find vector property
    emb_prop = next(p for p in info.properties if p.name == "embedding")
    assert "vector" in emb_prop.data_type.lower() or "Vector" in emb_prop.data_type


def test_add_list_properties_direct(empty_db):
    """Test adding list properties (list:string, list:int) via direct method."""
    db = empty_db

    db.create_label("Article")
    db.add_property("Article", "title", "string", False)
    db.add_property("Article", "tags", "list:string", False)
    db.add_property("Article", "scores", "list:int", False)

    info = db.get_label_info("Article")
    assert info is not None

    # Find list properties
    tags_prop = next(p for p in info.properties if p.name == "tags")
    scores_prop = next(p for p in info.properties if p.name == "scores")

    # Data type should contain "List" or "list"
    assert "list" in tags_prop.data_type.lower()
    assert "list" in scores_prop.data_type.lower()


# =============================================================================
# Schema Builder API
# =============================================================================


def test_schema_builder_single_label(empty_db):
    """Test schema builder for a single label."""
    db = empty_db

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .apply()
    )

    assert db.label_exists("Person")
    info = db.get_label_info("Person")
    assert info is not None
    assert len(info.properties) == 2
    assert {p.name for p in info.properties} == {"name", "age"}


def test_schema_builder_multiple_labels_edge_types(empty_db):
    """Test schema builder with multiple labels and edge types."""
    db = empty_db

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .label("Company")
        .property("name", "string")
        .property("founded", "int")
        .done()
        .edge_type("KNOWS", ["Person"], ["Person"])
        .done()
        .edge_type("WORKS_AT", ["Person"], ["Company"])
        .done()
        .apply()
    )

    assert db.label_exists("Person")
    assert db.label_exists("Company")
    assert db.edge_type_exists("KNOWS")
    assert db.edge_type_exists("WORKS_AT")


def test_schema_builder_label_apply_shortcut(empty_db):
    """Test schema builder with label apply() shortcut (no done())."""
    db = empty_db

    (
        db.schema()
        .label("Product")
        .property("name", "string")
        .property("price", "float")
        .apply()
    )

    assert db.label_exists("Product")
    info = db.get_label_info("Product")
    assert info is not None
    assert {p.name for p in info.properties} == {"name", "price"}


def test_schema_builder_nullable_properties(empty_db):
    """Test schema builder with nullable properties."""
    db = empty_db

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property_nullable("email", "string")
        .property_nullable("phone", "string")
        .done()
        .apply()
    )

    info = db.get_label_info("Person")
    assert info is not None

    nullable_props = {p.name for p in info.properties if p.nullable}
    assert nullable_props == {"email", "phone"}

    name_prop = next(p for p in info.properties if p.name == "name")
    assert not name_prop.nullable


def test_schema_builder_vector_with_index(empty_db):
    """Test schema builder with vector property and index."""
    db = empty_db

    (
        db.schema()
        .label("Document")
        .property("title", "string")
        .vector("embedding", 4)
        .index("title", "btree")
        .done()
        .apply()
    )

    assert db.label_exists("Document")
    info = db.get_label_info("Document")
    assert info is not None

    # Check for embedding property
    emb_prop = next(p for p in info.properties if p.name == "embedding")
    assert "vector" in emb_prop.data_type.lower() or "Vector" in emb_prop.data_type

    # Check for title index
    title_prop = next(p for p in info.properties if p.name == "title")
    assert title_prop.is_indexed


def test_schema_builder_edge_type_with_properties(empty_db):
    """Test schema builder with edge type containing properties."""
    db = empty_db

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .done()
        .edge_type("KNOWS", ["Person"], ["Person"])
        .property("since", "int")
        .property_nullable("strength", "float")
        .done()
        .apply()
    )

    assert db.edge_type_exists("KNOWS")

    # Insert data to verify edge properties work
    db.execute("CREATE (a:Person {name: 'Alice'})")
    db.execute("CREATE (b:Person {name: 'Bob'})")
    db.flush()
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
        "CREATE (a)-[:KNOWS {since: 2020, strength: 0.9}]->(b)"
    )
    db.flush()

    results = db.query(
        "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) "
        "RETURN r.since AS since, r.strength AS strength"
    )
    assert len(results) == 1
    assert results[0]["since"] == 2020
    assert results[0]["strength"] == pytest.approx(0.9)


# =============================================================================
# Schema Introspection
# =============================================================================


def test_label_exists_edge_type_exists(empty_db):
    """Test label_exists and edge_type_exists."""
    db = empty_db

    assert not db.label_exists("Person")
    assert not db.edge_type_exists("KNOWS")

    db.create_label("Person")
    db.create_edge_type("KNOWS", ["Person"], ["Person"])

    assert db.label_exists("Person")
    assert db.edge_type_exists("KNOWS")
    assert not db.label_exists("Company")
    assert not db.edge_type_exists("WORKS_AT")


def test_list_labels_list_edge_types(empty_db):
    """Test list_labels and list_edge_types."""
    db = empty_db

    assert db.list_labels() == []
    assert db.list_edge_types() == []

    db.create_label("Person")
    db.create_label("Company")
    db.create_edge_type("KNOWS", ["Person"], ["Person"])
    db.create_edge_type("WORKS_AT", ["Person"], ["Company"])

    labels = db.list_labels()
    assert set(labels) == {"Person", "Company"}

    edge_types = db.list_edge_types()
    assert set(edge_types) == {"KNOWS", "WORKS_AT"}


def test_get_label_info(empty_db):
    """Test get_label_info returns detailed label information."""
    db = empty_db

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .property_nullable("email", "string")
        .index("name", "btree")
        .done()
        .apply()
    )

    info = db.get_label_info("Person")
    assert info is not None
    assert info.name == "Person"
    assert info.count >= 0  # Initially 0

    # Check properties
    assert len(info.properties) == 3
    prop_dict = {p.name: p for p in info.properties}
    assert "name" in prop_dict
    assert "age" in prop_dict
    assert "email" in prop_dict

    # Check nullable flag
    assert not prop_dict["name"].nullable
    assert not prop_dict["age"].nullable
    assert prop_dict["email"].nullable

    # Check index
    assert prop_dict["name"].is_indexed

    # Non-existent label
    assert db.get_label_info("NonExistent") is None


def test_get_schema(empty_db):
    """Test get_schema returns complete schema dictionary."""
    db = empty_db

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .label("Company")
        .property("name", "string")
        .done()
        .edge_type("WORKS_AT", ["Person"], ["Company"])
        .done()
        .apply()
    )

    schema = db.get_schema()
    assert isinstance(schema, dict)

    # Schema should contain labels and edge types
    assert "labels" in schema or "vertices" in schema or len(schema) > 0


# =============================================================================
# Schema Persistence
# =============================================================================


def test_save_schema_load_schema(empty_db, tmp_path):
    """Test save_schema and load_schema for schema persistence."""
    db1 = empty_db

    # Define schema in first DB
    (
        db1.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .property_nullable("email", "string")
        .done()
        .label("Company")
        .property("name", "string")
        .done()
        .edge_type("WORKS_AT", ["Person"], ["Company"])
        .property_nullable("role", "string")
        .done()
        .apply()
    )

    # Save schema to file
    schema_path = tmp_path / "schema.json"
    db1.save_schema(str(schema_path))
    assert schema_path.exists()

    # Create new empty DB and load schema
    db2 = uni_db.DatabaseBuilder.temporary().build()
    assert not db2.label_exists("Person")
    assert not db2.label_exists("Company")

    db2.load_schema(str(schema_path))

    # Verify schema was loaded
    assert db2.label_exists("Person")
    assert db2.label_exists("Company")
    assert db2.edge_type_exists("WORKS_AT")

    info = db2.get_label_info("Person")
    assert info is not None
    prop_names = {p.name for p in info.properties}
    assert prop_names == {"name", "age", "email"}


# =============================================================================
# End-to-End Data Type Verification
# =============================================================================


def test_data_type_string_e2e(empty_db):
    """Test string data type end-to-end (insert + query)."""
    db = empty_db

    db.create_label("Person")
    db.add_property("Person", "name", "string", False)

    db.execute("CREATE (p:Person {name: 'Alice'})")
    db.flush()

    results = db.query("MATCH (p:Person) RETURN p.name AS name")
    assert len(results) == 1
    assert results[0]["name"] == "Alice"


def test_data_type_int_e2e(empty_db):
    """Test int data type end-to-end (insert + query)."""
    db = empty_db

    db.create_label("Person")
    db.add_property("Person", "age", "int", False)

    db.execute("CREATE (p:Person {age: 30})")
    db.flush()

    results = db.query("MATCH (p:Person) RETURN p.age AS age")
    assert len(results) == 1
    assert results[0]["age"] == 30


def test_data_type_float_e2e(empty_db):
    """Test float data type end-to-end (insert + query)."""
    db = empty_db

    db.create_label("Product")
    db.add_property("Product", "price", "float", False)

    db.execute("CREATE (p:Product {price: 99.99})")
    db.flush()

    results = db.query("MATCH (p:Product) RETURN p.price AS price")
    assert len(results) == 1
    assert results[0]["price"] == pytest.approx(99.99)


def test_data_type_bool_e2e(empty_db):
    """Test bool data type end-to-end (insert + query)."""
    db = empty_db

    db.create_label("User")
    db.add_property("User", "active", "bool", False)

    db.execute("CREATE (u:User {active: true})")
    db.execute("CREATE (u:User {active: false})")
    db.flush()

    results = db.query("MATCH (u:User) RETURN u.active AS active ORDER BY u.active")
    assert len(results) == 2
    assert results[0]["active"] is False
    assert results[1]["active"] is True


def test_data_type_vector_e2e(empty_db):
    """Test vector data type end-to-end (insert + query)."""
    db = empty_db

    (
        db.schema()
        .label("Document")
        .property("title", "string")
        .vector("embedding", 4)
        .done()
        .apply()
    )

    db.execute("CREATE (d:Document {title: 'Doc1', embedding: [1.0, 0.0, 0.0, 0.0]})")
    db.flush()

    results = db.query("MATCH (d:Document) RETURN d.embedding AS embedding")
    assert len(results) == 1
    assert isinstance(results[0]["embedding"], list)
    assert len(results[0]["embedding"]) == 4
    assert results[0]["embedding"][0] == pytest.approx(1.0)


def test_data_type_list_string_e2e(empty_db):
    """Test list:string data type end-to-end (insert + query)."""
    db = empty_db

    db.create_label("Article")
    db.add_property("Article", "tags", "list:string", False)

    db.execute("CREATE (a:Article {tags: ['python', 'database', 'graph']})")
    db.flush()

    results = db.query("MATCH (a:Article) RETURN a.tags AS tags")
    assert len(results) == 1
    assert isinstance(results[0]["tags"], list)
    assert set(results[0]["tags"]) == {"python", "database", "graph"}


def test_data_type_list_int_e2e(empty_db):
    """Test list:int data type end-to-end (insert + query)."""
    db = empty_db

    db.create_label("Data")
    db.add_property("Data", "scores", "list:int", False)

    db.execute("CREATE (d:Data {scores: [10, 20, 30, 40]})")
    db.flush()

    results = db.query("MATCH (d:Data) RETURN d.scores AS scores")
    assert len(results) == 1
    assert isinstance(results[0]["scores"], list)
    assert results[0]["scores"] == [10, 20, 30, 40]


def test_nullable_property_with_null_value_e2e(empty_db):
    """Test nullable property accepts null values end-to-end."""
    db = empty_db

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property_nullable("email", "string")
        .done()
        .apply()
    )

    # Create with null email
    db.execute("CREATE (p:Person {name: 'Alice'})")
    # Create with email
    db.execute("CREATE (p:Person {name: 'Bob', email: 'bob@example.com'})")
    db.flush()

    results = db.query(
        "MATCH (p:Person) RETURN p.name AS name, p.email AS email ORDER BY p.name"
    )
    assert len(results) == 2

    # Alice should have null email
    assert results[0]["name"] == "Alice"
    assert results[0]["email"] is None

    # Bob should have email
    assert results[1]["name"] == "Bob"
    assert results[1]["email"] == "bob@example.com"


def test_multiple_data_types_combined(empty_db):
    """Test combining multiple data types in one label."""
    db = empty_db

    (
        db.schema()
        .label("Product")
        .property("name", "string")
        .property("price", "float")
        .property("stock", "int")
        .property("available", "bool")
        .property_nullable("description", "string")
        .vector("embedding", 4)
        .done()
        .apply()
    )

    db.execute(
        "CREATE (p:Product {name: 'Laptop', price: 999.99, stock: 10, available: true, "
        "embedding: [1.0, 0.0, 0.0, 0.0]})"
    )
    db.execute(
        "CREATE (p:Product {name: 'Phone', price: 699.99, stock: 0, available: false, "
        "description: 'Out of stock', embedding: [0.0, 1.0, 0.0, 0.0]})"
    )
    db.flush()

    results = db.query(
        "MATCH (p:Product) RETURN p.name AS name, p.price AS price, p.stock AS stock, "
        "p.available AS available, p.description AS description, p.embedding AS embedding "
        "ORDER BY p.name"
    )
    assert len(results) == 2

    # Laptop
    assert results[0]["name"] == "Laptop"
    assert results[0]["price"] == pytest.approx(999.99)
    assert results[0]["stock"] == 10
    assert results[0]["available"] is True
    assert results[0]["description"] is None
    assert len(results[0]["embedding"]) == 4

    # Phone
    assert results[1]["name"] == "Phone"
    assert results[1]["price"] == pytest.approx(699.99)
    assert results[1]["stock"] == 0
    assert results[1]["available"] is False
    assert results[1]["description"] == "Out of stock"
    assert len(results[1]["embedding"]) == 4
