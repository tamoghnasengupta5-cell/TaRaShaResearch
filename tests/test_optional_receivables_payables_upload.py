import io
import unittest

import pandas as pd

from core_backend import (
    extract_annual_accounts_payable_series,
    extract_annual_accounts_receivable_series,
    extract_bs_annual_series_by_rowlabel,
    extract_latest_ttm_accounts_payable,
    extract_latest_ttm_accounts_receivable,
    extract_quarterly_accounts_receivable_series,
)


def _balance_sheet_workbook(*, include_optional_rows: bool) -> bytes:
    output = io.BytesIO()

    if include_optional_rows:
        annual = pd.DataFrame(
            {
                "Date": ["Accounts Receivable, Net", "Account Payables"],
                "2024-12-31": [101.0, 202.0],
                "2025-12-31": [None, 303.0],
            }
        )
        ttm = pd.DataFrame(
            {
                "Date": ["Accounts Receivable, Net", "Account Payables"],
                "2025-12-31": [111.0, 222.0],
                "2026-03-31": [None, 333.0],
            }
        )
        quarterly = pd.DataFrame(
            {
                "Date": ["Accounts Receivable, Net"],
                "2025-12-31": [44.0],
                "2026-03-31": [None],
            }
        )
    else:
        annual = pd.DataFrame(
            {
                "Date": ["Total Assets"],
                "2024-12-31": [1_000.0],
                "2025-12-31": [1_100.0],
            }
        )
        ttm = pd.DataFrame(
            {
                "Date": ["Total Assets"],
                "2025-12-31": [1_100.0],
                "2026-03-31": [1_200.0],
            }
        )
        quarterly = ttm.copy()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        annual.to_excel(writer, sheet_name="Balance-Sheet-Annual", index=False)
        ttm.to_excel(writer, sheet_name="Balance-Sheet-TTM", index=False)
        quarterly.to_excel(writer, sheet_name="Balance-Sheet-Quarterly", index=False)
    return output.getvalue()


class OptionalReceivablesPayablesUploadTests(unittest.TestCase):
    def test_missing_rows_default_to_zero_for_every_available_period(self):
        workbook = _balance_sheet_workbook(include_optional_rows=False)

        self.assertEqual(
            extract_annual_accounts_receivable_series(workbook),
            {2024: 0.0, 2025: 0.0},
        )
        self.assertEqual(
            extract_latest_ttm_accounts_receivable(workbook),
            ("2026-03-31", 0.0),
        )
        self.assertEqual(
            extract_quarterly_accounts_receivable_series(workbook),
            {"2026-03-31": 0.0, "2025-12-31": 0.0},
        )
        self.assertEqual(
            extract_annual_accounts_payable_series(workbook),
            {2024: 0.0, 2025: 0.0},
        )
        self.assertEqual(
            extract_latest_ttm_accounts_payable(workbook),
            ("2026-03-31", 0.0),
        )

    def test_existing_aliases_and_blank_cell_behavior_are_preserved(self):
        workbook = _balance_sheet_workbook(include_optional_rows=True)

        self.assertEqual(
            extract_annual_accounts_receivable_series(workbook),
            {2024: 101.0},
        )
        self.assertEqual(
            extract_latest_ttm_accounts_receivable(workbook),
            ("2025-12-31", 111.0),
        )
        self.assertEqual(
            extract_quarterly_accounts_receivable_series(workbook),
            {"2025-12-31": 44.0},
        )
        self.assertEqual(
            extract_annual_accounts_payable_series(workbook),
            {2024: 202.0, 2025: 303.0},
        )
        self.assertEqual(
            extract_latest_ttm_accounts_payable(workbook),
            ("2026-03-31", 333.0),
        )

    def test_other_balance_sheet_rows_remain_required_by_default(self):
        workbook = _balance_sheet_workbook(include_optional_rows=False)

        with self.assertRaisesRegex(ValueError, "Couldn't find any of"):
            extract_bs_annual_series_by_rowlabel(workbook, "Total Debt")


if __name__ == "__main__":
    unittest.main()
