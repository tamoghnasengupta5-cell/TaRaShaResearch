import sqlite3
import unittest

from core_backend import (
    get_annual_capital_expenditures_series,
    upsert_annual_capital_expenditures,
)
from ttc_efficiency import _capital_intensity, _free_cash_flow


class CapexSignContractTests(unittest.TestCase):
    def test_ttc_subtracts_positive_capex_outflow(self):
        self.assertEqual(_free_cash_flow(136_162, 64_551), 71_611)
        self.assertAlmostEqual(_capital_intensity(64_551, 245_122), 64_551 / 245_122)

    def test_ttc_handles_legacy_negative_capex_sign(self):
        self.assertEqual(_free_cash_flow(136_162, -64_551), 71_611)
        self.assertAlmostEqual(_capital_intensity(-64_551, 245_122), 64_551 / 245_122)

    def test_database_boundary_stores_and_reads_positive_outflow(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE capital_expenditures_annual (
                company_id INTEGER NOT NULL,
                fiscal_year INTEGER NOT NULL,
                capital_expenditures REAL NOT NULL,
                PRIMARY KEY (company_id, fiscal_year)
            )
            """
        )

        upsert_annual_capital_expenditures(conn, 1, {2025: -64_551})

        stored = conn.execute(
            "SELECT capital_expenditures FROM capital_expenditures_annual"
        ).fetchone()[0]
        self.assertEqual(stored, 64_551)
        self.assertEqual(get_annual_capital_expenditures_series(conn, 1), {2025: 64_551})


if __name__ == "__main__":
    unittest.main()
