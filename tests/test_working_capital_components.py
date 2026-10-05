import io
import unittest

import pandas as pd

from core_backend import (
    extract_current_portion_long_term_debt_series,
    extract_short_term_borrowings_series,
)


def _balance_sheet_workbook(*, include_short_term: bool = True) -> bytes:
    output = io.BytesIO()
    labels = ["Current Portion of Long-Term Debt", "Current Debt"]
    annual_2024 = [18.0, 40.0]
    annual_2025 = [20.0, 45.0]
    ttm_2025 = [20.0, 45.0]
    ttm_2026 = [22.0, 50.0]
    if include_short_term:
        labels.insert(0, "Short-Term Debt")
        annual_2024.insert(0, 12.0)
        annual_2025.insert(0, 15.0)
        ttm_2025.insert(0, 15.0)
        ttm_2026.insert(0, 17.0)

    annual = pd.DataFrame({"Date": labels, "2024-12-31": annual_2024, "2025-12-31": annual_2025})
    ttm = pd.DataFrame({"Date": labels, "2025-12-31": ttm_2025, "2026-03-31": ttm_2026})
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        annual.to_excel(writer, sheet_name="Balance-Sheet-Annual", index=False)
        ttm.to_excel(writer, sheet_name="Balance-Sheet-TTM", index=False)
    return output.getvalue()


class WorkingCapitalComponentExtractionTests(unittest.TestCase):
    def test_extracts_reported_borrowing_components_without_losing_ttm_period(self):
        workbook = _balance_sheet_workbook()

        short_term_annual, short_term_ttm = extract_short_term_borrowings_series(workbook)
        current_portion_annual, current_portion_ttm = extract_current_portion_long_term_debt_series(workbook)

        self.assertEqual(short_term_annual, {2024: 12.0, 2025: 15.0})
        self.assertEqual(short_term_ttm, ("2026-03-31", 17.0))
        self.assertEqual(current_portion_annual, {2024: 18.0, 2025: 20.0})
        self.assertEqual(current_portion_ttm, ("2026-03-31", 22.0))

    def test_missing_component_remains_unavailable_instead_of_becoming_zero(self):
        workbook = _balance_sheet_workbook(include_short_term=False)

        short_term_annual, short_term_ttm = extract_short_term_borrowings_series(workbook)

        self.assertEqual(short_term_annual, {})
        self.assertEqual(short_term_ttm, ("2026-03-31", None))


if __name__ == "__main__":
    unittest.main()
