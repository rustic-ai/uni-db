import os
import shutil
import sys
import unittest

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
import uni_db


class TestAlgoShowcase(unittest.TestCase):
    def setUp(self):
        self.test_dir = "./test_db_algo"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        self.db = uni_db.Database(self.test_dir)
        self.setup_data()

    def tearDown(self):
        del self.db
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def setup_data(self):
        self.db.create_label("Page")
        self.db.create_edge_type("LINKS")

        # Create a small graph for PageRank
        # A -> B, A -> C, B -> C, C -> A (cycle), D -> C (dangling source)

        nodes = ["A", "B", "C", "D"]
        for n in nodes:
            self.db.query(f"CREATE (:Page {{name: '{n}'}})")

        edges = [("A", "B"), ("A", "C"), ("B", "C"), ("C", "A"), ("D", "C")]

        for src, dst in edges:
            self.db.query(
                "MATCH (a:Page {name: $src}), (b:Page {name: $dst}) CREATE (a)-[:LINKS]->(b)",
                {"src": src, "dst": dst},
            )

    def test_pagerank(self):
        # CALL algo.pageRank('Page', 'LINKS') YIELD node, score
        # Assuming this syntax.

        results = self.db.query(
            "CALL algo.pageRank('Page', 'LINKS') YIELD node, score RETURN node.name as name, score ORDER BY score DESC"
        )

        print("PageRank Results:")
        for r in results:
            print(f" - {r['name']}: {r['score']}")

        # C has many incoming links (A, B, D), should be high.
        # A is linked by C.
        self.assertTrue(len(results) == 4)

    def test_wcc(self):
        # Create disjoint component
        self.db.query("CREATE (:Page {name: 'E'})-[:LINKS]->(:Page {name: 'F'})")

        results = self.db.query(
            "CALL algo.wcc('Page', 'LINKS') YIELD node, componentId RETURN node.name as name, componentId ORDER BY componentId"
        )

        print("WCC Results:")
        components = {}
        for r in results:
            c = r["componentId"]
            if c not in components:
                components[c] = []
            components[c].append(r["name"])

        print(components)
        self.assertEqual(len(components), 2)  # {A,B,C,D} and {E,F}

    def test_shortest_path(self):
        # D -> C -> A -> B
        results = self.db.query(
            "MATCH p = shortestPath((s:Page {name: 'D'})-[:LINKS*]->(t:Page {name: 'B'})) RETURN length(p) as len, [n in nodes(p) | n.name] as path"
        )

        print("Shortest Path D->B:")
        for r in results:
            print(f"Len: {r['len']}, Path: {r['path']}")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["len"], 3)
        self.assertEqual(results[0]["path"], ["D", "C", "A", "B"])


if __name__ == "__main__":
    unittest.main()
