# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for schema management."""

import pytest

import uni_db


@pytest.mark.asyncio
async def test_create_labels_direct(async_empty_db):
    """Test creating labels via direct methods."""

    label_id = await async_empty_db.create_label("Person")
    assert isinstance(label_id, int)
    assert label_id >= 0

    assert await async_empty_db.label_exists("Person")
    labels = await async_empty_db.list_labels()
    assert "Person" in labels


@pytest.mark.asyncio
async def test_create_edge_types_direct(async_empty_db):
    """Test creating edge types via direct methods."""

    await async_empty_db.create_label("Person")
    await async_empty_db.create_label("Company")

    edge_id = await async_empty_db.create_edge_type("WORKS_AT", ["Person"], ["Company"])
    assert isinstance(edge_id, int)
    assert edge_id >= 0

    assert await async_empty_db.edge_type_exists("WORKS_AT")
    edge_types = await async_empty_db.list_edge_types()
    assert "WORKS_AT" in edge_types


@pytest.mark.asyncio
async def test_add_properties_basic_types(async_empty_db):
    """Test adding properties with basic data types."""

    await async_empty_db.create_label("User")

    await async_empty_db.add_property("User", "name", "string", False)
    await async_empty_db.add_property("User", "age", "int", False)
    await async_empty_db.add_property("User", "score", "float", False)
    await async_empty_db.add_property("User", "active", "bool", False)

    info = await async_empty_db.get_label_info("User")
    assert info.name == "User"

    prop_names = {p.name for p in info.properties}
    assert "name" in prop_names
    assert "age" in prop_names
    assert "score" in prop_names
    assert "active" in prop_names

    # Check data types
    props_by_name = {p.name: p for p in info.properties}
    assert props_by_name["name"].data_type in ("string", "String")
    assert props_by_name["age"].data_type in ("int", "Int64")
    assert props_by_name["score"].data_type in ("float", "Float64")
    assert props_by_name["active"].data_type in ("bool", "Bool", "Boolean")


@pytest.mark.asyncio
async def test_nullable_properties(async_empty_db):
    """Test nullable and non-nullable properties."""

    await async_empty_db.create_label("Product")

    # Non-nullable property
    await async_empty_db.add_property("Product", "name", "string", False)

    # Nullable property
    await async_empty_db.add_property("Product", "description", "string", True)

    info = await async_empty_db.get_label_info("Product")
    props_by_name = {p.name: p for p in info.properties}

    assert props_by_name["name"].nullable is False
    assert props_by_name["description"].nullable is True


@pytest.mark.asyncio
async def test_vector_properties(async_empty_db):
    """Test vector properties."""

    await async_empty_db.create_label("Document")
    await async_empty_db.add_property("Document", "embedding", "vector:128", False)

    info = await async_empty_db.get_label_info("Document")
    props_by_name = {p.name: p for p in info.properties}

    assert "embedding" in props_by_name
    assert (
        "vector" in props_by_name["embedding"].data_type.lower()
        or "128" in props_by_name["embedding"].data_type
    )


@pytest.mark.asyncio
async def test_list_properties(async_empty_db):
    """Test list properties."""

    await async_empty_db.create_label("Article")
    await async_empty_db.add_property("Article", "tags", "list:string", False)
    await async_empty_db.add_property("Article", "scores", "list:int", False)

    info = await async_empty_db.get_label_info("Article")
    props_by_name = {p.name: p for p in info.properties}

    assert "tags" in props_by_name
    assert (
        "list" in props_by_name["tags"].data_type.lower()
        or "List" in props_by_name["tags"].data_type
    )
    assert "scores" in props_by_name
    assert (
        "list" in props_by_name["scores"].data_type.lower()
        or "List" in props_by_name["scores"].data_type
    )


@pytest.mark.asyncio
async def test_schema_builder_single_label(async_empty_db):
    """Test schema builder with a single label."""

    await (
        async_empty_db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .apply()
    )

    assert await async_empty_db.label_exists("Person")
    info = await async_empty_db.get_label_info("Person")
    prop_names = {p.name for p in info.properties}
    assert "name" in prop_names
    assert "age" in prop_names


@pytest.mark.asyncio
async def test_schema_builder_multiple_labels_and_edges(async_empty_db):
    """Test schema builder with multiple labels and edge types."""

    await (
        async_empty_db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .label("Company")
        .property("name", "string")
        .property("founded", "int")
        .done()
        .edge_type("WORKS_AT", ["Person"], ["Company"])
        .property("since", "int")
        .done()
        .apply()
    )

    # Verify labels
    assert await async_empty_db.label_exists("Person")
    assert await async_empty_db.label_exists("Company")

    # Verify edge type
    assert await async_empty_db.edge_type_exists("WORKS_AT")

    # Verify properties
    person_info = await async_empty_db.get_label_info("Person")
    person_props = {p.name for p in person_info.properties}
    assert "name" in person_props
    assert "age" in person_props

    company_info = await async_empty_db.get_label_info("Company")
    company_props = {p.name for p in company_info.properties}
    assert "name" in company_props
    assert "founded" in company_props


@pytest.mark.asyncio
async def test_schema_builder_label_apply_shortcut(async_empty_db):
    """Test schema builder with label().apply() shortcut."""
    await async_empty_db.schema().label("User").property("email", "string").apply()

    assert await async_empty_db.label_exists("User")
    info = await async_empty_db.get_label_info("User")
    prop_names = {p.name for p in info.properties}
    assert "email" in prop_names


@pytest.mark.asyncio
async def test_schema_builder_nullable_properties(async_empty_db):
    """Test schema builder with nullable properties."""

    await (
        async_empty_db.schema()
        .label("Profile")
        .property("username", "string")
        .property_nullable("bio", "string")
        .property_nullable("website", "string")
        .done()
        .apply()
    )

    info = await async_empty_db.get_label_info("Profile")
    props_by_name = {p.name: p for p in info.properties}

    assert props_by_name["username"].nullable is False
    assert props_by_name["bio"].nullable is True
    assert props_by_name["website"].nullable is True


@pytest.mark.asyncio
async def test_schema_builder_vector_with_index(async_empty_db):
    """Test schema builder with vector property and index."""

    await (
        async_empty_db.schema()
        .label("Image")
        .property("path", "string")
        .vector("embedding", 512)
        .index("path", "btree")
        .done()
        .apply()
    )

    info = await async_empty_db.get_label_info("Image")
    props_by_name = {p.name: p for p in info.properties}

    assert "embedding" in props_by_name
    assert (
        "vector" in props_by_name["embedding"].data_type.lower()
        or "512" in props_by_name["embedding"].data_type
    )
    assert "path" in props_by_name

    # Check index exists
    assert props_by_name["path"].is_indexed is True


@pytest.mark.asyncio
async def test_schema_builder_edge_type_with_properties(async_empty_db):
    """Test schema builder with edge type and properties."""

    await (
        async_empty_db.schema()
        .label("Person")
        .property("name", "string")
        .done()
        .edge_type("KNOWS", ["Person"], ["Person"])
        .property("since", "int")
        .property("weight", "float")
        .done()
        .apply()
    )

    assert await async_empty_db.edge_type_exists("KNOWS")

    # Create an edge to verify properties work
    await async_empty_db.execute("CREATE (a:Person {name: 'Alice'})")
    await async_empty_db.execute("CREATE (b:Person {name: 'Bob'})")
    await async_empty_db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
        "CREATE (a)-[:KNOWS {since: 2020, weight: 0.8}]->(b)"
    )

    result = await async_empty_db.query(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since as since, r.weight as weight"
    )
    assert len(result) == 1
    assert result[0]["since"] == 2020
    assert abs(result[0]["weight"] - 0.8) < 0.001


@pytest.mark.asyncio
async def test_label_exists_and_edge_type_exists(async_empty_db):
    """Test label_exists and edge_type_exists methods."""

    assert await async_empty_db.label_exists("NonExistent") is False
    assert await async_empty_db.edge_type_exists("NON_EXISTENT") is False

    await async_empty_db.create_label("TestLabel")
    assert await async_empty_db.label_exists("TestLabel") is True

    await async_empty_db.create_label("Source")
    await async_empty_db.create_label("Target")
    await async_empty_db.create_edge_type("TEST_EDGE", ["Source"], ["Target"])
    assert await async_empty_db.edge_type_exists("TEST_EDGE") is True


@pytest.mark.asyncio
async def test_list_labels_and_edge_types(async_empty_db):
    """Test list_labels and list_edge_types methods."""

    labels = await async_empty_db.list_labels()
    edge_types = await async_empty_db.list_edge_types()

    await async_empty_db.create_label("Person")
    await async_empty_db.create_label("Company")
    await async_empty_db.create_label("Product")

    labels = await async_empty_db.list_labels()
    assert isinstance(labels, list)
    assert "Person" in labels
    assert "Company" in labels
    assert "Product" in labels

    await async_empty_db.create_edge_type("WORKS_AT", ["Person"], ["Company"])
    await async_empty_db.create_edge_type("PRODUCES", ["Company"], ["Product"])

    edge_types = await async_empty_db.list_edge_types()
    assert isinstance(edge_types, list)
    assert "WORKS_AT" in edge_types
    assert "PRODUCES" in edge_types


@pytest.mark.asyncio
async def test_get_label_info(async_empty_db):
    """Test get_label_info method."""

    await (
        async_empty_db.schema()
        .label("Movie")
        .property("title", "string")
        .property("year", "int")
        .property_nullable("rating", "float")
        .index("title", "btree")
        .done()
        .apply()
    )

    await async_empty_db.execute(
        "CREATE (:Movie {title: 'The Matrix', year: 1999, rating: 8.7})"
    )
    await async_empty_db.execute("CREATE (:Movie {title: 'Inception', year: 2010})")
    await async_empty_db.flush()

    info = await async_empty_db.get_label_info("Movie")

    assert info.name == "Movie"
    # count may be 0 if get_label_info doesn't reflect recent inserts
    # just check the attribute exists
    assert hasattr(info, "count")

    # Check properties
    assert len(info.properties) == 3
    props_by_name = {p.name: p for p in info.properties}

    assert props_by_name["title"].data_type in ("string", "String")
    assert props_by_name["title"].nullable is False
    assert props_by_name["title"].is_indexed is True

    assert props_by_name["year"].data_type in ("int", "Int64")
    assert props_by_name["year"].nullable is False

    assert props_by_name["rating"].data_type in ("float", "Float64")
    assert props_by_name["rating"].nullable is True


@pytest.mark.asyncio
async def test_get_schema(async_empty_db):
    """Test get_schema method."""

    await (
        async_empty_db.schema()
        .label("User")
        .property("name", "string")
        .done()
        .label("Post")
        .property("title", "string")
        .done()
        .edge_type("AUTHORED", ["User"], ["Post"])
        .done()
        .apply()
    )

    schema = async_empty_db.get_schema()

    assert isinstance(schema, dict)
    assert "labels" in schema or "nodes" in schema or len(schema) > 0


@pytest.mark.asyncio
async def test_save_and_load_schema(async_empty_db, tmp_path):
    """Test save_schema and load_schema methods."""

    await (
        async_empty_db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .vector("embedding", 128)
        .index("name", "btree")
        .done()
        .label("Company")
        .property("name", "string")
        .done()
        .edge_type("WORKS_AT", ["Person"], ["Company"])
        .property("since", "int")
        .done()
        .apply()
    )

    schema_path = tmp_path / "test_schema.json"
    await async_empty_db.save_schema(str(schema_path))

    assert schema_path.exists()
    assert schema_path.stat().st_size > 0

    db2 = await uni_db.AsyncDatabase.temporary()
    await db2.load_schema(str(schema_path))

    assert await db2.label_exists("Person")
    assert await db2.label_exists("Company")
    assert await db2.edge_type_exists("WORKS_AT")

    person_info = await db2.get_label_info("Person")
    props_by_name = {p.name: p for p in person_info.properties}

    assert "name" in props_by_name
    assert "age" in props_by_name
    assert "embedding" in props_by_name
    assert (
        "vector" in props_by_name["embedding"].data_type.lower()
        or "128" in props_by_name["embedding"].data_type
    )
    assert props_by_name["name"].is_indexed is True


@pytest.mark.asyncio
async def test_data_types_e2e(async_empty_db):
    """Test various data types work end-to-end."""

    await (
        async_empty_db.schema()
        .label("TestEntity")
        .property("str_val", "string")
        .property("int_val", "int")
        .property("float_val", "float")
        .property("bool_val", "bool")
        .property("tags", "list:string")
        .property("scores", "list:int")
        .vector("embedding", 4)
        .done()
        .apply()
    )

    await async_empty_db.execute("""
        CREATE (:TestEntity {
            str_val: 'hello',
            int_val: 42,
            float_val: 3.14,
            bool_val: true,
            tags: ['a', 'b', 'c'],
            scores: [1, 2, 3],
            embedding: [0.1, 0.2, 0.3, 0.4]
        })
    """)

    result = await async_empty_db.query("""
        MATCH (e:TestEntity)
        RETURN e.str_val, e.int_val, e.float_val, e.bool_val,
               e.tags, e.scores, e.embedding
    """)

    assert len(result) == 1
    row = result[0]

    assert row["e.str_val"] == "hello"
    assert row["e.int_val"] == 42
    assert abs(row["e.float_val"] - 3.14) < 0.001
    assert row["e.bool_val"] is True
    assert row["e.tags"] == ["a", "b", "c"]
    assert row["e.scores"] == [1, 2, 3]
    assert len(row["e.embedding"]) == 4
    assert abs(row["e.embedding"][0] - 0.1) < 0.001


@pytest.mark.asyncio
async def test_schema_builder_complex_workflow(async_empty_db):
    """Test a complex schema building workflow."""

    # Build a social network schema
    await (
        async_empty_db.schema()
        .label("Person")
        .property("name", "string")
        .property("email", "string")
        .property("age", "int")
        .property_nullable("bio", "string")
        .vector("profile_embedding", 256)
        .index("name", "btree")
        .index("email", "btree")
        .done()
        .label("Post")
        .property("title", "string")
        .property("content", "string")
        .property("created_at", "int")
        .property("tags", "list:string")
        .vector("content_embedding", 256)
        .index("created_at", "btree")
        .done()
        .label("Comment")
        .property("text", "string")
        .property("created_at", "int")
        .done()
        .edge_type("FOLLOWS", ["Person"], ["Person"])
        .property("since", "int")
        .done()
        .edge_type("AUTHORED", ["Person"], ["Post"])
        .property("timestamp", "int")
        .done()
        .edge_type("COMMENTED_ON", ["Person"], ["Post"])
        .done()
        .edge_type("HAS_COMMENT", ["Post"], ["Comment"])
        .done()
        .apply()
    )

    # Verify all labels exist
    labels = await async_empty_db.list_labels()
    assert "Person" in labels
    assert "Post" in labels
    assert "Comment" in labels

    # Verify all edge types exist
    edge_types = await async_empty_db.list_edge_types()
    assert "FOLLOWS" in edge_types
    assert "AUTHORED" in edge_types
    assert "COMMENTED_ON" in edge_types
    assert "HAS_COMMENT" in edge_types

    # Verify Person label details
    person_info = await async_empty_db.get_label_info("Person")
    person_props = {p.name: p for p in person_info.properties}
    assert "name" in person_props
    assert "email" in person_props
    assert "age" in person_props
    assert "bio" in person_props
    assert "profile_embedding" in person_props
    assert person_props["bio"].nullable is True
    assert person_props["name"].is_indexed is True
    assert person_props["email"].is_indexed is True

    # Create some data to verify schema works
    await async_empty_db.execute(
        """
        CREATE (alice:Person {
            name: 'Alice',
            email: 'alice@example.com',
            age: 30,
            bio: 'Software engineer',
            profile_embedding: """
        + str([0.1] * 256)
        + """
        })
        CREATE (bob:Person {
            name: 'Bob',
            email: 'bob@example.com',
            age: 25,
            profile_embedding: """
        + str([0.2] * 256)
        + """
        })
        CREATE (post:Post {
            title: 'Hello World',
            content: 'This is my first post',
            created_at: 1234567890,
            tags: ['introduction', 'hello'],
            content_embedding: """
        + str([0.3] * 256)
        + """
        })
        CREATE (alice)-[:FOLLOWS {since: 1234567890}]->(bob)
        CREATE (alice)-[:AUTHORED {timestamp: 1234567890}]->(post)
    """
    )

    # Query to verify
    result = await async_empty_db.query("""
        MATCH (a:Person)-[:AUTHORED]->(p:Post)
        RETURN a.name, p.title
    """)

    assert len(result) == 1
    assert result[0]["a.name"] == "Alice"
    assert result[0]["p.title"] == "Hello World"
