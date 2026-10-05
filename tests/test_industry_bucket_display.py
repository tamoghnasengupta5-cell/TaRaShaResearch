import sqlite3
import unittest

import pandas as pd

from core_backend import (
    compute_and_store_levered_beta,
    get_company_valuation_industry_beta,
    get_company_valuation_industry_bucket_map,
)
from ui_theme import add_taxonomy_pictures, display_table_frame


class IndustryBucketDisplayTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            """
            CREATE TABLE companies (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                ticker TEXT NOT NULL,
                country TEXT
            );
            CREATE TABLE company_groups (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE company_group_members (
                group_id INTEGER NOT NULL,
                company_id INTEGER NOT NULL,
                PRIMARY KEY (group_id, company_id)
            );
            CREATE TABLE industry_betas (
                user_industry_bucket TEXT NOT NULL,
                mapped_sector TEXT NOT NULL,
                unlevered_beta REAL NOT NULL,
                cash_adjusted_beta REAL NOT NULL,
                PRIMARY KEY (user_industry_bucket, mapped_sector)
            );
            CREATE TABLE marginal_corporate_tax_rates (
                country TEXT PRIMARY KEY,
                effective_rate REAL NOT NULL
            );
            CREATE TABLE debt_equity_annual (
                company_id INTEGER NOT NULL,
                fiscal_year INTEGER NOT NULL,
                debt_equity REAL NOT NULL,
                PRIMARY KEY (company_id, fiscal_year)
            );
            CREATE TABLE levered_beta_annual (
                company_id INTEGER NOT NULL,
                fiscal_year INTEGER NOT NULL,
                levered_beta REAL NOT NULL,
                PRIMARY KEY (company_id, fiscal_year)
            );
            """
        )
        self.conn.execute(
            "INSERT INTO companies(id, name, ticker, country) VALUES(1, 'Example Co', 'EX', 'USA')"
        )
        self.conn.executemany(
            "INSERT INTO company_groups(id, name) VALUES(?, ?)",
            [
                (10, "Legacy Industry"),
                (20, "Current Industry"),
                (30, "Custom Comparison Group"),
            ],
        )
        self.conn.executemany(
            "INSERT INTO company_group_members(group_id, company_id) VALUES(?, 1)",
            [(10,), (20,), (30,)],
        )
        self.conn.executemany(
            """
            INSERT INTO industry_betas(
                user_industry_bucket, mapped_sector, unlevered_beta, cash_adjusted_beta
            ) VALUES(?, ?, ?, ?)
            """,
            [
                ("Legacy Industry", "Legacy", 0.8, 0.9),
                ("Current Industry", "Current", 1.2, 1.3),
            ],
        )
        self.conn.execute(
            "INSERT INTO marginal_corporate_tax_rates(country, effective_rate) VALUES('USA', 25.0)"
        )
        self.conn.execute(
            "INSERT INTO debt_equity_annual(company_id, fiscal_year, debt_equity) VALUES(1, 2025, 0.5)"
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_dashboard_bucket_matches_single_beta_used_by_valuation(self):
        self.assertEqual(
            get_company_valuation_industry_bucket_map(self.conn, [1]),
            {1: "Current Industry"},
        )
        self.assertAlmostEqual(
            get_company_valuation_industry_beta(self.conn, 1, "unlevered_beta"),
            1.2,
        )

        compute_and_store_levered_beta(self.conn, 1)
        stored_beta = self.conn.execute(
            "SELECT levered_beta FROM levered_beta_annual WHERE company_id = 1 AND fiscal_year = 2025"
        ).fetchone()[0]
        self.assertAlmostEqual(stored_beta, 1.2 * (1.0 + (1.0 - 0.25) * 0.5))

    def test_industry_columns_do_not_receive_taxonomy_icons(self):
        frame = pd.DataFrame(
            {
                "Industry Bucket": ["Technology : Semiconductors"],
                "Industry": ["Technology : Semiconductors"],
                "Category": ["Technology"],
            }
        )

        themed = add_taxonomy_pictures(frame)

        self.assertEqual(themed.loc[0, "Industry Bucket"], "Technology : Semiconductors")
        self.assertEqual(themed.loc[0, "Industry"], "Technology : Semiconductors")
        self.assertNotEqual(themed.loc[0, "Category"], "Technology")

        displayed = display_table_frame(frame[["Industry Bucket"]])
        self.assertEqual(displayed.loc[0, "Industry"], "Technology : Semiconductors")


if __name__ == "__main__":
    unittest.main()
