import io
import unittest
from collections import defaultdict

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import db_models
from core_backend import (
    extract_annual_common_dividends_paid_series,
    extract_annual_earnings_from_discontinued_operations_series,
    extract_annual_minority_interest_in_earnings_series,
    extract_annual_net_income_to_common_series,
    extract_latest_ttm_common_dividends_paid,
    extract_latest_ttm_earnings_from_discontinued_operations,
    extract_latest_ttm_minority_interest_in_earnings,
    extract_latest_ttm_net_income_to_common,
    extract_quarterly_common_dividends_paid_series,
    extract_quarterly_earnings_from_discontinued_operations_series,
    extract_quarterly_minority_interest_in_earnings_series,
    extract_quarterly_net_income_to_common_series,
    upsert_company,
)
from data_upload import _persist_parsed_company_metrics
from db_session import DbCompat, ManagedSession


def _statement_workbook() -> bytes:
    output = io.BytesIO()
    income_annual = pd.DataFrame(
        {
            "Date": [
                "Earnings From Discontinued Operations",
                "Minority Interest in Earnings",
                "Net Income to Common",
            ],
            "2024-12-31": [-12.0, 6.0, -30.0],
            "2025-12-31": [4.0, -3.0, 45.0],
        }
    )
    income_quarterly = pd.DataFrame(
        {
            "Date": [
                "Earnings From Discontinued Operations",
                "Minority Interest in Earnings",
                "Net Income to Common",
            ],
            "2025-12-31": [-2.0, 1.5, -8.0],
            "2026-03-31": [3.0, -0.5, 12.0],
        }
    )
    income_ttm = pd.DataFrame(
        {
            "Date": [
                "Earnings From Discontinued Operations",
                "Minority Interest in Earnings",
                "Net Income to Common",
            ],
            "2025-12-31": [8.0, 2.0, 40.0],
            "2026-03-31": [-7.0, -1.0, -15.0],
        }
    )
    cash_flow_annual = pd.DataFrame(
        {
            "Date": ["Common Dividends Paid"],
            "2024-12-31": [-20.0],
            "2025-12-31": [-25.0],
        }
    )
    cash_flow_quarterly = pd.DataFrame(
        {
            "Date": ["Common Dividends Paid"],
            "2025-12-31": [-6.0],
            "2026-03-31": [-7.0],
        }
    )
    cash_flow_ttm = pd.DataFrame(
        {
            "Date": ["Common Dividends Paid"],
            "2025-12-31": [-25.0],
            "2026-03-31": [-28.0],
        }
    )

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        income_annual.to_excel(writer, sheet_name="Income-Annual", index=False)
        income_quarterly.to_excel(writer, sheet_name="Income-Quarterly", index=False)
        income_ttm.to_excel(writer, sheet_name="Income-TTM", index=False)
        cash_flow_annual.to_excel(writer, sheet_name="Cash-Flow-Annual", index=False)
        cash_flow_quarterly.to_excel(writer, sheet_name="Cash-Flow-Quarterly", index=False)
        cash_flow_ttm.to_excel(writer, sheet_name="Cash-Flow-TTM", index=False)
    return output.getvalue()


def _statement_workbook_with_missing_values(*, omit_rows: bool) -> bytes:
    output = io.BytesIO()

    def frame(labels, first_value, second_value):
        return pd.DataFrame(
            {
                "Date": labels,
                "2025-12-31": first_value,
                "2026-03-31": second_value,
            }
        )

    if omit_rows:
        income_labels = ["Revenue"]
        income_values = ([100.0], [110.0])
        cash_labels = ["Operating Cash Flow"]
        cash_values = ([20.0], [25.0])
    else:
        income_labels = [
            "Earnings From Discontinued Operations",
            "Minority Interest in Earnings",
        ]
        income_values = ([5.0, -2.0], [None, None])
        cash_labels = ["Common Dividends Paid"]
        cash_values = ([-8.0], [None])

    income = frame(income_labels, *income_values)
    cash_flow = frame(cash_labels, *cash_values)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet in ("Income-Annual", "Income-Quarterly", "Income-TTM"):
            income.to_excel(writer, sheet_name=sheet, index=False)
        for sheet in ("Cash-Flow-Annual", "Cash-Flow-Quarterly", "Cash-Flow-TTM"):
            cash_flow.to_excel(writer, sheet_name=sheet, index=False)
    return output.getvalue()


def _statement_workbook_without_quarterly_cash_flow_sheet() -> bytes:
    output = io.BytesIO()
    income_quarterly = pd.DataFrame(
        {
            "Date": ["Revenue"],
            "2025-12-31": [100.0],
            "2026-03-31": [110.0],
        }
    )
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        income_quarterly.to_excel(writer, sheet_name="Income-Quarterly", index=False)
    return output.getvalue()


class AdditionalBulkUploadStatementItemTests(unittest.TestCase):
    def test_extractors_preserve_earnings_signs_and_normalize_dividend_outflows(self):
        workbook = _statement_workbook()

        self.assertEqual(
            extract_annual_earnings_from_discontinued_operations_series(workbook),
            {2024: -12.0, 2025: 4.0},
        )
        self.assertEqual(
            extract_quarterly_earnings_from_discontinued_operations_series(workbook),
            {"2026-03-31": 3.0, "2025-12-31": -2.0},
        )
        self.assertEqual(
            extract_latest_ttm_earnings_from_discontinued_operations(workbook),
            ("2026-03-31", -7.0),
        )

        self.assertEqual(
            extract_annual_minority_interest_in_earnings_series(workbook),
            {2024: 6.0, 2025: -3.0},
        )
        self.assertEqual(
            extract_quarterly_minority_interest_in_earnings_series(workbook),
            {"2026-03-31": -0.5, "2025-12-31": 1.5},
        )
        self.assertEqual(
            extract_latest_ttm_minority_interest_in_earnings(workbook),
            ("2026-03-31", -1.0),
        )

        self.assertEqual(
            extract_annual_common_dividends_paid_series(workbook),
            {2024: 20.0, 2025: 25.0},
        )
        self.assertEqual(
            extract_quarterly_common_dividends_paid_series(workbook),
            {"2026-03-31": 7.0, "2025-12-31": 6.0},
        )
        self.assertEqual(
            extract_latest_ttm_common_dividends_paid(workbook),
            ("2026-03-31", 28.0),
        )
        self.assertEqual(
            extract_annual_net_income_to_common_series(workbook),
            {2024: -30.0, 2025: 45.0},
        )
        self.assertEqual(
            extract_quarterly_net_income_to_common_series(workbook),
            {"2026-03-31": 12.0, "2025-12-31": -8.0},
        )
        self.assertEqual(
            extract_latest_ttm_net_income_to_common(workbook),
            ("2026-03-31", -15.0),
        )

    def test_missing_rows_are_uploaded_as_zero_for_every_period(self):
        workbook = _statement_workbook_with_missing_values(omit_rows=True)

        self.assertEqual(
            extract_annual_earnings_from_discontinued_operations_series(workbook),
            {2025: 0.0, 2026: 0.0},
        )
        self.assertEqual(
            extract_quarterly_earnings_from_discontinued_operations_series(workbook),
            {"2026-03-31": 0.0, "2025-12-31": 0.0},
        )
        self.assertEqual(
            extract_latest_ttm_earnings_from_discontinued_operations(workbook),
            ("2026-03-31", 0.0),
        )
        self.assertEqual(
            extract_annual_minority_interest_in_earnings_series(workbook),
            {2025: 0.0, 2026: 0.0},
        )
        self.assertEqual(
            extract_quarterly_minority_interest_in_earnings_series(workbook),
            {"2026-03-31": 0.0, "2025-12-31": 0.0},
        )
        self.assertEqual(
            extract_latest_ttm_minority_interest_in_earnings(workbook),
            ("2026-03-31", 0.0),
        )
        self.assertEqual(
            extract_annual_common_dividends_paid_series(workbook),
            {2025: 0.0, 2026: 0.0},
        )
        self.assertEqual(
            extract_quarterly_common_dividends_paid_series(workbook),
            {"2026-03-31": 0.0, "2025-12-31": 0.0},
        )
        self.assertEqual(
            extract_latest_ttm_common_dividends_paid(workbook),
            ("2026-03-31", 0.0),
        )
        self.assertEqual(
            extract_annual_net_income_to_common_series(workbook),
            {2025: 0.0, 2026: 0.0},
        )
        self.assertEqual(
            extract_quarterly_net_income_to_common_series(workbook),
            {"2026-03-31": 0.0, "2025-12-31": 0.0},
        )
        self.assertEqual(
            extract_latest_ttm_net_income_to_common(workbook),
            ("2026-03-31", 0.0),
        )

    def test_blank_cells_are_uploaded_as_zero_including_latest_ttm(self):
        workbook = _statement_workbook_with_missing_values(omit_rows=False)

        self.assertEqual(
            extract_annual_earnings_from_discontinued_operations_series(workbook),
            {2025: 5.0, 2026: 0.0},
        )
        self.assertEqual(
            extract_quarterly_minority_interest_in_earnings_series(workbook),
            {"2026-03-31": 0.0, "2025-12-31": -2.0},
        )
        self.assertEqual(
            extract_latest_ttm_earnings_from_discontinued_operations(workbook),
            ("2026-03-31", 0.0),
        )
        self.assertEqual(
            extract_annual_common_dividends_paid_series(workbook),
            {2025: 8.0, 2026: 0.0},
        )
        self.assertEqual(
            extract_quarterly_common_dividends_paid_series(workbook),
            {"2026-03-31": 0.0, "2025-12-31": 8.0},
        )
        self.assertEqual(
            extract_latest_ttm_common_dividends_paid(workbook),
            ("2026-03-31", 0.0),
        )

    def test_missing_quarterly_cash_flow_sheet_uses_income_quarter_columns(self):
        workbook = _statement_workbook_without_quarterly_cash_flow_sheet()
        self.assertEqual(
            extract_quarterly_common_dividends_paid_series(workbook),
            {"2026-03-31": 0.0, "2025-12-31": 0.0},
        )

    def test_bulk_persistence_uses_separate_annual_and_quarterly_tables(self):
        engine = create_engine(
            "sqlite://",
            future=True,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
        db_models.metadata.create_all(engine)
        factory = sessionmaker(
            bind=engine,
            class_=ManagedSession,
            autoflush=False,
            expire_on_commit=False,
            future=True,
        )
        conn = DbCompat(factory())
        try:
            with conn.transaction():
                company_id = upsert_company(conn, "Example", "EX")
                values = defaultdict(dict)
                values.update(
                    {
                        # The 2026 values represent the latest TTM values merged
                        # into the annual series by the ingestion path.
                        "annual_earnings_from_discontinued_operations": {2025: 4.0, 2026: -7.0},
                        "annual_minority_interest_in_earnings": {2025: -3.0, 2026: -1.0},
                        "annual_common_dividends_paid": {2025: 25.0, 2026: 28.0},
                        "annual_net_income_to_common": {2025: 45.0, 2026: -15.0},
                        "quarterly_earnings_from_discontinued_operations": {"2026-03-31": 3.0},
                        "quarterly_minority_interest_in_earnings": {"2026-03-31": -0.5},
                        "quarterly_common_dividends_paid": {"2026-03-31": 7.0},
                        "quarterly_net_income_to_common": {"2026-03-31": 12.0},
                    }
                )
                _persist_parsed_company_metrics(conn, company_id, values)

            checks = [
                (
                    "earnings_from_discontinued_operations_annual",
                    "fiscal_year",
                    "earnings_from_discontinued_operations",
                    [(2025, 4.0), (2026, -7.0)],
                ),
                (
                    "minority_interest_in_earnings_annual",
                    "fiscal_year",
                    "minority_interest_in_earnings",
                    [(2025, -3.0), (2026, -1.0)],
                ),
                (
                    "common_dividends_paid_annual",
                    "fiscal_year",
                    "common_dividends_paid",
                    [(2025, 25.0), (2026, 28.0)],
                ),
                (
                    "net_income_to_common_annual",
                    "fiscal_year",
                    "net_income_to_common",
                    [(2025, 45.0), (2026, -15.0)],
                ),
                (
                    "earnings_from_discontinued_operations_quarterly",
                    "quarter_end",
                    "earnings_from_discontinued_operations",
                    [("2026-03-31", 3.0)],
                ),
                (
                    "minority_interest_in_earnings_quarterly",
                    "quarter_end",
                    "minority_interest_in_earnings",
                    [("2026-03-31", -0.5)],
                ),
                (
                    "common_dividends_paid_quarterly",
                    "quarter_end",
                    "common_dividends_paid",
                    [("2026-03-31", 7.0)],
                ),
                (
                    "net_income_to_common_quarterly",
                    "quarter_end",
                    "net_income_to_common",
                    [("2026-03-31", 12.0)],
                ),
            ]
            for table, period_column, value_column, expected in checks:
                rows = conn.execute(
                    f"SELECT {period_column}, {value_column} FROM {table} "
                    f"WHERE company_id = ? ORDER BY {period_column}",
                    (company_id,),
                ).fetchall()
                self.assertEqual(rows, expected)
        finally:
            conn.close()
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
