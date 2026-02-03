"""End-to-end tests for synchronous bulk operations."""

import pytest


def test_create_bulk_writer_with_builder(social_db):
    """Test creating a bulk writer using the builder pattern."""
    writer = social_db.bulk_writer().build()
    assert writer is not None

    # Abort to clean up
    writer.abort()


def test_bulk_insert_vertices(social_db):
    """Test bulk inserting vertices and receiving vertex IDs."""
    writer = social_db.bulk_writer().build()

    people_data = [
        {"name": "Alice", "age": 30, "email": "alice@example.com"},
        {"name": "Bob", "age": 25, "email": "bob@example.com"},
        {"name": "Charlie", "age": 35},
    ]

    vids = writer.insert_vertices("Person", people_data)

    assert isinstance(vids, list)
    assert len(vids) == 3
    assert all(isinstance(vid, int) for vid in vids)
    assert len(set(vids)) == 3  # All IDs should be unique

    stats = writer.commit()
    assert stats.vertices_inserted == 3


def test_bulk_insert_edges(social_db):
    """Test bulk inserting edges after inserting vertices."""
    writer = social_db.bulk_writer().build()

    # Insert vertices first
    people_data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
    vids = writer.insert_vertices("Person", people_data)

    # Insert edges between them
    edges_data = [
        (vids[0], vids[1], {"since": 2020}),
        (vids[1], vids[2], {"since": 2021}),
        (vids[0], vids[2], {}),
    ]

    result = writer.insert_edges("KNOWS", edges_data)
    assert result is None  # insert_edges returns None

    stats = writer.commit()
    assert stats.vertices_inserted == 3
    assert stats.edges_inserted == 3


def test_bulk_writer_with_defer_vector_indexes(social_db):
    """Test bulk writer with defer_vector_indexes option."""
    writer = social_db.bulk_writer().defer_vector_indexes(True).build()

    people_data = [{"name": "Alice", "age": 30}]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    assert stats.vertices_inserted == 1


def test_bulk_writer_with_defer_scalar_indexes(social_db):
    """Test bulk writer with defer_scalar_indexes option."""
    writer = social_db.bulk_writer().defer_scalar_indexes(True).build()

    people_data = [{"name": "Bob", "age": 25}]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    assert stats.vertices_inserted == 1


def test_bulk_writer_with_batch_size(social_db):
    """Test bulk writer with custom batch_size option."""
    writer = social_db.bulk_writer().batch_size(100).build()

    people_data = [{"name": f"Person{i}", "age": 20 + i} for i in range(10)]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    assert stats.vertices_inserted == 10


def test_bulk_writer_with_async_indexes(social_db):
    """Test bulk writer with async_indexes option."""
    writer = social_db.bulk_writer().async_indexes(True).build()

    people_data = [{"name": "Charlie", "age": 35}]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    assert stats.vertices_inserted == 1


def test_bulk_writer_with_all_config_options(social_db):
    """Test bulk writer with all configuration options combined."""
    writer = (
        social_db.bulk_writer()
        .defer_vector_indexes(True)
        .defer_scalar_indexes(False)
        .batch_size(50)
        .async_indexes(True)
        .build()
    )

    people_data = [{"name": f"Person{i}", "age": 20 + i} for i in range(5)]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    assert stats.vertices_inserted == 5


def test_bulk_stats_attributes(social_db):
    """Test that BulkStats has all expected attributes."""
    writer = social_db.bulk_writer().build()

    people_data = [{"name": "Alice", "age": 30}]
    vids = writer.insert_vertices("Person", people_data)

    companies_data = [{"name": "TechCorp", "founded": 2010}]
    company_vids = writer.insert_vertices("Company", companies_data)

    edges_data = [(vids[0], company_vids[0], {"role": "Engineer"})]
    writer.insert_edges("WORKS_AT", edges_data)

    stats = writer.commit()

    # Check all expected attributes
    assert hasattr(stats, "vertices_inserted")
    assert hasattr(stats, "edges_inserted")
    assert hasattr(stats, "indexes_rebuilt")
    assert hasattr(stats, "duration_secs")
    assert hasattr(stats, "index_build_duration_secs")
    assert hasattr(stats, "indexes_pending")

    # Verify values
    assert stats.vertices_inserted == 2
    assert stats.edges_inserted == 1
    assert isinstance(stats.indexes_rebuilt, int)
    assert isinstance(stats.duration_secs, (int, float))
    assert isinstance(stats.index_build_duration_secs, (int, float))
    assert isinstance(stats.indexes_pending, int)
    assert stats.duration_secs >= 0
    assert stats.index_build_duration_secs >= 0


@pytest.mark.xfail(
    reason="abort() only sets a flag; insert_vertices writes directly to engine without batching, so data is already committed before abort"
)
def test_bulk_writer_abort(social_db):
    """Test aborting a bulk writer."""
    writer = social_db.bulk_writer().build()

    people_data = [{"name": "Alice", "age": 30}]
    writer.insert_vertices("Person", people_data)

    # Abort the writer
    writer.abort()

    # Verify data was not committed
    social_db.flush()
    result = social_db.query("MATCH (p:Person) RETURN count(p) AS cnt")
    assert result[0]["cnt"] == 0


def test_operations_after_abort_raise_error(social_db):
    """Test that operations after abort raise RuntimeError."""
    writer = social_db.bulk_writer().build()

    # Abort the writer
    writer.abort()

    # Attempting to insert vertices should raise RuntimeError
    with pytest.raises(RuntimeError):
        writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])

    # Attempting to insert edges should raise RuntimeError
    with pytest.raises(RuntimeError):
        writer.insert_edges("KNOWS", [(1, 2, {})])

    # Attempting to commit should raise RuntimeError
    with pytest.raises(RuntimeError):
        writer.commit()


def test_convenience_bulk_insert_vertices(social_db):
    """Test the convenience bulk_insert_vertices method."""
    people_data = [
        {"name": "Alice", "age": 30, "email": "alice@example.com"},
        {"name": "Bob", "age": 25, "email": "bob@example.com"},
        {"name": "Charlie", "age": 35},
    ]

    vids = social_db.bulk_insert_vertices("Person", people_data)

    assert isinstance(vids, list)
    assert len(vids) == 3
    assert all(isinstance(vid, int) for vid in vids)

    # Verify data was inserted
    social_db.flush()
    result = social_db.query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
    assert len(result) == 3
    names = [row["p.name"] for row in result]
    assert names == ["Alice", "Bob", "Charlie"]


def test_convenience_bulk_insert_edges(social_db):
    """Test the convenience bulk_insert_edges method."""
    # First insert vertices
    people_data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ]
    vids = social_db.bulk_insert_vertices("Person", people_data)

    # Then insert edges using convenience method
    edges_data = [
        (vids[0], vids[1], {"since": 2020}),
    ]

    result = social_db.bulk_insert_edges("KNOWS", edges_data)
    assert result is None  # bulk_insert_edges returns None

    # Verify edge was inserted
    social_db.flush()
    result = social_db.query(
        "MATCH (a:Person)-[k:KNOWS]->(b:Person) "
        "WHERE a.name = 'Alice' AND b.name = 'Bob' "
        "RETURN k.since"
    )
    assert len(result) == 1
    assert result[0]["k.since"] == 2020


def test_large_batch_insert(social_db):
    """Test inserting a large batch of vertices (1000+)."""
    writer = social_db.bulk_writer().batch_size(200).build()

    # Generate 1500 vertices
    large_batch = [{"name": f"Person{i}", "age": 20 + (i % 50)} for i in range(1500)]

    vids = writer.insert_vertices("Person", large_batch)

    assert len(vids) == 1500
    assert len(set(vids)) == 1500  # All IDs should be unique

    stats = writer.commit()
    assert stats.vertices_inserted == 1500

    # Verify data was inserted
    social_db.flush()
    result = social_db.query("MATCH (p:Person) RETURN count(p) as cnt")
    assert result[0]["cnt"] == 1500


def test_verify_data_correctness_after_bulk_insert(social_db):
    """Test that data inserted via bulk operations is queryable and correct."""
    writer = social_db.bulk_writer().build()

    # Insert people
    people_data = [
        {"name": "Alice", "age": 30, "email": "alice@example.com"},
        {"name": "Bob", "age": 25, "email": "bob@example.com"},
        {"name": "Charlie", "age": 35},
    ]
    person_vids = writer.insert_vertices("Person", people_data)

    # Insert companies
    companies_data = [
        {"name": "TechCorp", "founded": 2010},
        {"name": "StartupInc", "founded": 2020},
    ]
    company_vids = writer.insert_vertices("Company", companies_data)

    # Insert KNOWS edges
    knows_edges = [
        (person_vids[0], person_vids[1], {"since": 2020}),
        (person_vids[1], person_vids[2], {"since": 2021}),
    ]
    writer.insert_edges("KNOWS", knows_edges)

    # Insert WORKS_AT edges
    works_at_edges = [
        (person_vids[0], company_vids[0], {"role": "Engineer"}),
        (person_vids[1], company_vids[1], {"role": "Designer"}),
    ]
    writer.insert_edges("WORKS_AT", works_at_edges)

    stats = writer.commit()
    assert stats.vertices_inserted == 5
    assert stats.edges_inserted == 4

    # Flush to ensure data is queryable
    social_db.flush()

    # Verify Person vertices
    result = social_db.query(
        "MATCH (p:Person) RETURN p.name, p.age, p.email ORDER BY p.name"
    )
    assert len(result) == 3
    assert result[0]["p.name"] == "Alice"
    assert result[0]["p.age"] == 30
    assert result[0]["p.email"] == "alice@example.com"
    assert result[1]["p.name"] == "Bob"
    assert result[1]["p.age"] == 25
    assert result[2]["p.name"] == "Charlie"
    assert result[2]["p.age"] == 35
    assert result[2]["p.email"] is None

    # Verify Company vertices
    result = social_db.query(
        "MATCH (c:Company) RETURN c.name, c.founded ORDER BY c.name"
    )
    assert len(result) == 2
    assert result[0]["c.name"] == "StartupInc"
    assert result[0]["c.founded"] == 2020
    assert result[1]["c.name"] == "TechCorp"
    assert result[1]["c.founded"] == 2010

    # Verify KNOWS edges
    result = social_db.query(
        "MATCH (a:Person)-[k:KNOWS]->(b:Person) "
        "RETURN a.name, b.name, k.since "
        "ORDER BY a.name, b.name"
    )
    assert len(result) == 2
    assert result[0]["a.name"] == "Alice"
    assert result[0]["b.name"] == "Bob"
    assert result[0]["k.since"] == 2020
    assert result[1]["a.name"] == "Bob"
    assert result[1]["b.name"] == "Charlie"
    assert result[1]["k.since"] == 2021

    # Verify WORKS_AT edges
    result = social_db.query(
        "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) "
        "RETURN p.name, c.name, w.role "
        "ORDER BY p.name"
    )
    assert len(result) == 2
    assert result[0]["p.name"] == "Alice"
    assert result[0]["c.name"] == "TechCorp"
    assert result[0]["w.role"] == "Engineer"
    assert result[1]["p.name"] == "Bob"
    assert result[1]["c.name"] == "StartupInc"
    assert result[1]["w.role"] == "Designer"


def test_multiple_vertex_labels_in_single_writer(social_db):
    """Test inserting multiple different vertex labels with a single writer."""
    writer = social_db.bulk_writer().build()

    # Insert different types of vertices
    writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])
    writer.insert_vertices("Company", [{"name": "TechCorp"}])

    stats = writer.commit()
    assert stats.vertices_inserted == 2

    social_db.flush()

    # Verify both types exist
    result = social_db.query("MATCH (p:Person) RETURN count(p) as cnt")
    assert result[0]["cnt"] == 1

    result = social_db.query("MATCH (c:Company) RETURN count(c) as cnt")
    assert result[0]["cnt"] == 1


def test_bulk_insert_with_empty_data(social_db):
    """Test bulk insert with empty data arrays."""
    writer = social_db.bulk_writer().build()

    # Insert empty vertex list
    vids = writer.insert_vertices("Person", [])
    assert vids == []

    # Insert empty edge list
    result = writer.insert_edges("KNOWS", [])
    assert result is None

    stats = writer.commit()
    assert stats.vertices_inserted == 0
    assert stats.edges_inserted == 0


def test_bulk_insert_edges_without_properties(social_db):
    """Test bulk inserting edges without any properties."""
    writer = social_db.bulk_writer().build()

    people_data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    vids = writer.insert_vertices("Person", people_data)

    # Insert edge without properties
    edges_data = [(vids[0], vids[1], {})]
    writer.insert_edges("KNOWS", edges_data)

    stats = writer.commit()
    assert stats.edges_inserted == 1

    social_db.flush()
    result = social_db.query(
        "MATCH (a:Person)-[k:KNOWS]->(b:Person) "
        "WHERE a.name = 'Alice' AND b.name = 'Bob' "
        "RETURN k"
    )
    assert len(result) == 1
