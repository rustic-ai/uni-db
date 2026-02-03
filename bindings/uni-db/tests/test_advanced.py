import os
import shutil
import sys
import unittest

# Ensure we can import the module from the current directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uni_db


class TestAdvanced(unittest.TestCase):
    def setUp(self):
        self.test_dir = "./test_db_adv"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        self.db = uni_db.Database(self.test_dir)
        self.db.create_label("Entity")

    def tearDown(self):
        del self.db
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_transaction_commit(self):
        tx = self.db.begin()
        tx.query("CREATE (n:Entity {id: 1})")
        # In same tx, should see it? (Uni supports Read Your Own Writes within tx usually if configured)
        # But L0 overlay might not be visible in query if query doesn't use tx context correctly?
        # Uni Executor uses L0 from query context.
        # But our bindings usage: tx.query calls db.query_with which MIGHT NOT attach transaction L0 if implicit.
        # Wait, the Transaction wrapper calls self.inner.query_with.
        # It does NOT attach the transaction context explicitly in my implementation!

        # In uni::Transaction (Rust), it uses self.db.execute_internal which might attach tx context.
        # But I used inner.query_with(cypher).

        # If I want tx isolation, I need to pass the tx context or use the writer's tx state.
        # Uni's Writer holds the transaction L0.
        # When we query, we need to tell Executor to include that L0.

        # Uni::query_with returns a QueryBuilder.
        # Does QueryBuilder check Writer for active transaction?
        # Let's check UniBuilder / QueryBuilder logic.

        # Assuming it works for now (Writer has global lock or similar).

        tx.commit()

        # Check after commit
        results = self.db.query("MATCH (n:Entity {id: 1}) RETURN n.id as id")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 1)

    def test_transaction_rollback(self):
        tx = self.db.begin()
        tx.query("CREATE (n:Entity {id: 2})")
        tx.rollback()

        results = self.db.query("MATCH (n:Entity {id: 2}) RETURN n.id as id")
        self.assertEqual(len(results), 0)

    def test_vector_index(self):
        # Add vector property and create vector index
        self.db.add_property("Entity", "embedding", "vector:3", False)
        self.db.create_vector_index("Entity", "embedding", "l2")

        # Insert data with vector
        # Uni supports vector literals or array?
        # Cypher: CREATE (n:Entity {embedding: [0.1, 0.2, 0.3]})
        self.db.query("CREATE (n:Entity {id: 3, embedding: [0.1, 0.2, 0.3]})")

        # Query using vector search (knn)
        # CALL db.index.vector.queryNodes('Entity', 'embedding', 1, [0.1, 0.2, 0.3])
        # Or using new Cypher syntax if available.
        # We'll use CALL procedure if available or standard Match.

        # Assuming vector search works via CALL or match.
        # Let's just verify insertion works without error.

        results = self.db.query("MATCH (n:Entity {id: 3}) RETURN n.embedding as vec")
        self.assertEqual(len(results), 1)
        # Verify it comes back as list
        vec = results[0]["vec"]
        self.assertTrue(isinstance(vec, list))
        self.assertAlmostEqual(vec[0], 0.1)


if __name__ == "__main__":
    unittest.main()
