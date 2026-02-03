import os
import shutil
import sys
import unittest

# Ensure we can import the module from the current directory
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

import uni_db


class TestUseCases(unittest.TestCase):
    def setUp(self):
        self.test_dir_base = "./test_db_use_cases"
        if os.path.exists(self.test_dir_base):
            shutil.rmtree(self.test_dir_base)
        os.makedirs(self.test_dir_base, exist_ok=True)

    def get_db(self, name):
        path = os.path.join(self.test_dir_base, name)
        return uni_db.Database(path)

    def test_supply_chain(self):
        db = self.get_db("supply_chain")

        # 1. Setup Schema
        db.create_label("Part")
        db.create_label("Supplier")
        db.create_label("Product")

        db.create_edge_type("ASSEMBLED_FROM", ["Product", "Part"], ["Part"])
        db.create_edge_type("SUPPLIED_BY", ["Part"], ["Supplier"])

        db.add_property("Part", "sku", "string", False)
        db.add_property("Part", "cost", "float64", False)
        db.add_property("Product", "name", "string", False)
        db.add_property("Product", "price", "float64", False)

        db.create_scalar_index("Part", "sku", "hash")

        # 2. Ingestion
        p1_props = {
            "sku": "RES-10K",
            "cost": 0.05,
            "_doc": {"type": "resistor", "compliance": ["RoHS"]},
        }
        p2_props = {"sku": "MB-X1", "cost": 50.0}
        p3_props = {"sku": "SCR-OLED", "cost": 30.0}

        vids = db.bulk_insert_vertices("Part", [p1_props, p2_props, p3_props])
        p1, p2, p3 = vids

        prod_props = {"name": "Smartphone X", "price": 500.0}
        phone_vids = db.bulk_insert_vertices("Product", [prod_props])
        phone = phone_vids[0]

        db.bulk_insert_edges(
            "ASSEMBLED_FROM", [(phone, p2, {}), (phone, p3, {}), (p2, p1, {})]
        )

        db.flush()

        # Warm-up query to ensure all adjacency partitions are loaded (workaround for engine bug)
        db.query("MATCH (a:Part)-[:ASSEMBLED_FROM]->(b:Part) RETURN a.sku")

        # 3. BOM Explosion
        query = """
            MATCH (defective:Part {sku: 'RES-10K'})
            MATCH (product:Product)-[:ASSEMBLED_FROM*1..5]->(defective)
            RETURN product.name as name, product.price as price
        """
        results = db.query(query)
        names = [r.get("name") for r in results]
        self.assertIn("Smartphone X", names)

        # 4. Cost Rollup
        query_cost = """
            MATCH (p:Product {name: 'Smartphone X'})
            MATCH (p)-[:ASSEMBLED_FROM*1..5]->(part:Part)
            RETURN SUM(part.cost) AS total_bom_cost
        """
        results_cost = db.query(query_cost)
        self.assertEqual(len(results_cost), 1)
        # cost = 50 (p2) + 30 (p3) + 0.05 (p1) = 80.05
        self.assertAlmostEqual(results_cost[0]["total_bom_cost"], 80.05)

    def test_recommendation(self):
        db = self.get_db("recommendation")

        # 1. Schema
        db.create_label("User")
        db.create_label("Product")
        db.create_label("Category")

        db.create_edge_type("VIEWED", ["User"], ["Product"])
        db.create_edge_type("PURCHASED", ["User"], ["Product"])
        db.create_edge_type("IN_CATEGORY", ["Product"], ["Category"])

        db.add_property("Product", "name", "string", False)
        db.add_property("Product", "price", "float64", False)

        db.create_vector_index("Product", "embedding", "cosine")

        # 2. Ingestion
        p1_vec = [1.0, 0.0, 0.0, 0.0]
        p2_vec = [0.9, 0.1, 0.0, 0.0]

        vids = db.bulk_insert_vertices(
            "Product",
            [
                {"name": "Running Shoes", "price": 100.0, "embedding": p1_vec},
                {"name": "Socks", "price": 10.0, "embedding": p2_vec},
            ],
        )
        p1, p2 = vids

        u_vids = db.bulk_insert_vertices("User", [{}, {}, {}])
        u1, u2, u3 = u_vids

        db.bulk_insert_edges("PURCHASED", [(u1, p1, {}), (u2, p1, {}), (u3, p1, {})])

        db.flush()

        # 3. Collaborative Filter
        # Who else bought what Alice (u1) bought?
        query = """
            MATCH (u1:User)-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(other:User)
            WHERE u1._vid = $uid AND other._vid <> u1._vid
            RETURN count(DISTINCT other) as count
        """
        results = db.query(query, {"uid": u1})
        self.assertEqual(results[0]["count"], 2)  # u2 and u3

    def test_rag(self):
        db = self.get_db("rag")

        # 1. Schema
        db.create_label("Chunk")
        db.create_label("Entity")
        db.create_edge_type("MENTIONS", ["Chunk"], ["Entity"])

        db.add_property("Chunk", "text", "string", False)
        db.create_vector_index("Chunk", "embedding", "cosine")

        # 2. Ingestion
        c1_vec = [1.0, 0.0, 0.0, 0.0]
        c2_vec = [0.9, 0.1, 0.0, 0.0]

        c_vids = db.bulk_insert_vertices(
            "Chunk",
            [
                {"text": "Function verify() checks signatures.", "embedding": c1_vec},
                {"text": "Other text about verify.", "embedding": c2_vec},
            ],
        )
        c1, c2 = c_vids

        e_vids = db.bulk_insert_vertices(
            "Entity", [{"name": "verify", "type": "function"}]
        )
        e1 = e_vids[0]

        db.bulk_insert_edges("MENTIONS", [(c1, e1, {}), (c2, e1, {})])
        db.flush()

        # 3. Hybrid RAG Query
        # Find related chunks via topic for a given chunk
        query = """
            MATCH (c:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)
            WHERE c._vid = $cid AND related._vid <> c._vid
            RETURN related.text as text
        """
        results = db.query(query, {"cid": c1})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["text"], "Other text about verify.")

    def test_fraud_detection(self):
        db = self.get_db("fraud")

        # 1. Schema
        db.create_label("User")
        db.create_label("Device")
        db.create_edge_type("SENT_MONEY", ["User"], ["User"])
        db.create_edge_type("USED_DEVICE", ["User"], ["Device"])
        db.add_property("SENT_MONEY", "amount", "float64", False)
        db.add_property("User", "risk_score", "float32", True)

        # 2. Ingestion
        u_vids = db.bulk_insert_vertices(
            "User",
            [
                {"risk_score": 0.1},  # A
                {"risk_score": 0.2},  # B
                {"risk_score": 0.3},  # C
                {"risk_score": 0.9},  # D (Fraudster)
            ],
        )
        ua, ub, uc, ud = u_vids

        d_vids = db.bulk_insert_vertices("Device", [{}])
        d1 = d_vids[0]

        db.bulk_insert_edges(
            "SENT_MONEY",
            [
                (ua, ub, {"amount": 5000.0}),
                (ub, uc, {"amount": 5000.0}),
                (uc, ua, {"amount": 5000.0}),
            ],
        )

        db.bulk_insert_edges("USED_DEVICE", [(ua, d1, {}), (ud, d1, {})])
        db.flush()

        # 3. Cycle Detection
        query_cycle = """
            MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)
            RETURN count(*) as count
        """
        results = db.query(query_cycle)
        # 3 rotations of the same cycle
        self.assertEqual(results[0]["count"], 3)

        # 4. Shared Device with Fraudster
        query_shared = """
            MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)
            WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid
            RETURN u._vid as uid
        """
        results = db.query(query_shared)
        self.assertEqual(len(results), 1)
        uid = results[0].get("uid")
        self.assertEqual(uid, ua)

    def test_regional_sales_analytics(self):
        db = self.get_db("sales")

        db.create_label("Region")
        db.create_label("Order")
        db.create_edge_type("SHIPPED_TO", ["Order"], ["Region"])
        db.add_property("Region", "name", "string", False)
        db.add_property("Order", "amount", "float64", False)

        vids_region = db.bulk_insert_vertices("Region", [{"name": "North"}])
        north = vids_region[0]

        orders = [{"amount": 10.0 * (i + 1)} for i in range(100)]
        vids_orders = db.bulk_insert_vertices("Order", orders)

        edges = [(v, north, {}) for v in vids_orders]
        db.bulk_insert_edges("SHIPPED_TO", edges)
        db.flush()

        # Query: Sum of amounts for orders shipped to "North"
        query = """
            MATCH (r:Region {name: 'North'})<-[:SHIPPED_TO]-(o:Order)
            RETURN SUM(o.amount) as total
        """
        results = db.query(query)
        # Sum 1..100 = 5050. Total = 5050 * 10 = 50500
        self.assertAlmostEqual(results[0]["total"], 50500.0)

    def test_document_knowledge_graph(self):
        db = self.get_db("doc_kg")

        db.create_label("Paper")
        db.create_edge_type("CITES", ["Paper"], ["Paper"])

        db.add_property("Paper", "topic", "string", False)
        db.add_property("Paper", "title", "string", False)

        vids = db.bulk_insert_vertices(
            "Paper",
            [
                {"topic": "AI", "title": "Paper 1"},
                {"topic": "DB", "title": "Paper 2"},
                {"topic": "AI", "title": "Paper 3"},
            ],
        )
        p1, p2, p3 = vids

        db.bulk_insert_edges("CITES", [(p1, p3, {})])
        db.flush()

        # Find AI papers that cite other AI papers
        query = """
            MATCH (a:Paper {topic: 'AI'})-[:CITES]->(b:Paper {topic: 'AI'})
            RETURN a.title as src, b.title as dst
        """
        results = db.query(query)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["src"], "Paper 1")
        self.assertEqual(results[0]["dst"], "Paper 3")

    def test_ecommerce_recommendation(self):
        db = self.get_db("ecommerce")

        db.create_label("User")
        db.create_label("Product")
        db.create_edge_type("VIEWED", ["User"], ["Product"])

        db.add_property("User", "name", "string", False)
        db.add_property("Product", "name", "string", False)
        db.add_property("Product", "embedding", "vector:2", False)
        db.create_vector_index("Product", "embedding", "l2")

        # Alice viewed a Laptop
        vids_u = db.bulk_insert_vertices("User", [{"name": "Alice"}])
        alice = vids_u[0]

        vids_p = db.bulk_insert_vertices(
            "Product",
            [
                {"name": "Laptop", "embedding": [1.0, 0.0]},
                {"name": "Mouse", "embedding": [0.9, 0.1]},
                {"name": "Shampoo", "embedding": [0.0, 1.0]},
            ],
        )
        laptop, mouse, shampoo = vids_p

        db.bulk_insert_edges("VIEWED", [(alice, laptop, {})])
        db.flush()

        # 1. Find Alice's viewed products and their embeddings
        res = db.query(
            "MATCH (u:User {name: 'Alice'})-[:VIEWED]->(p:Product) RETURN p.embedding as emb"
        )
        self.assertEqual(len(res), 1)
        emb = res[0]["emb"]

        # 2. Find products similar to the laptop
        # Using vector_similarity in MATCH
        query = """
            MATCH (p:Product)
            WHERE vector_similarity(p.embedding, $emb) > 0.9
            RETURN p.name as name
        """
        res_sim = db.query(query, {"emb": emb})
        # Should find Laptop (sim 1.0) and Mouse (sim high)
        names = [r["name"] for r in res_sim]
        self.assertIn("Laptop", names)
        self.assertIn("Mouse", names)
        self.assertNotIn("Shampoo", names)

    def test_identity_provenance(self):
        db = self.get_db("provenance")

        db.create_label("Node")
        db.add_property("Node", "name", "string", False)
        db.create_edge_type("DERIVED_FROM", ["Node"], ["Node"])

        # 1. Ingestion via CREATE
        db.query("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'})")
        db.query(
            "MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (b)-[:DERIVED_FROM]->(a)"
        )
        db.flush()

        # 2. Query and traverse
        res = db.query(
            "MATCH (b:Node {name: 'B'})-[:DERIVED_FROM]->(a:Node) RETURN a.name as name"
        )
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["name"], "A")


if __name__ == "__main__":
    unittest.main()
