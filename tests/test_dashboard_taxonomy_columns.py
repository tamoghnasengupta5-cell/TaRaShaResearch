import sqlite3
import unittest

import pandas as pd

from core_backend import add_company_taxonomy_columns


class DashboardTaxonomyColumnsTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            """
            CREATE TABLE relative_valuation_categories (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE relative_valuation_subcategories (
                id INTEGER PRIMARY KEY, category_id INTEGER, name TEXT
            );
            CREATE TABLE relative_valuation_company_assignments (
                company_id INTEGER, subcategory_id INTEGER
            );
            INSERT INTO relative_valuation_categories VALUES (1, 'Industrial'), (2, 'Energy');
            INSERT INTO relative_valuation_subcategories VALUES
                (1, 1, 'Cables'), (2, 1, 'Motors'), (3, 2, 'Power');
            INSERT INTO relative_valuation_company_assignments VALUES
                (11, 2), (11, 1), (11, 1), (11, 3);
            """
        )

    def tearDown(self):
        self.conn.close()

    def test_all_dashboard_shapes_preserve_identity_order_scores_and_missing_labels(self):
        for company_column in ("Company", "Company Name"):
            for identity_columns in ((company_column, "Ticker"), ("Ticker", company_column)):
                with self.subTest(identity_columns=identity_columns):
                    rows = [
                        {"_company_id": 11, company_column: "Same Name", "Ticker": "DUP", "Score": 88.5},
                        {"_company_id": 12, company_column: "Same Name", "Ticker": "DUP", "Score": 75.0},
                    ]
                    original = pd.DataFrame(rows)[["_company_id", *identity_columns, "Score"]]
                    result = add_company_taxonomy_columns(self.conn, original)

                    self.assertEqual(
                        result.columns.tolist(),
                        [*identity_columns, "Category", "Sub-Category", "Score"],
                    )
                    self.assertEqual(result["Score"].tolist(), [88.5, 75.0])
                    self.assertEqual(result["Category"].tolist(), ["Energy, Industrial", ""])
                    self.assertEqual(result["Sub-Category"].tolist(), ["Cables, Motors, Power", ""])
                    self.assertEqual(result["Ticker"].tolist(), ["DUP", "DUP"])
                    self.assertEqual(original.columns.tolist(), ["_company_id", *identity_columns, "Score"])


if __name__ == "__main__":
    unittest.main()
