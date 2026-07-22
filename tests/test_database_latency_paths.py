import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import db_models
from core_backend import (
    bulk_upsert_company_metrics,
    get_combined_dashboard_series_batch,
    upsert_company,
)
from db_session import DbCompat, ManagedSession
from ttc_efficiency import (
    _compute_value_creation_filter_metrics,
    _load_ttc_combined_series_batch,
    _merge_ttm_into_annual,
)


class DatabaseLatencyPathTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            future=True,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
        db_models.metadata.create_all(self.engine)
        factory = sessionmaker(
            bind=self.engine,
            class_=ManagedSession,
            autoflush=False,
            expire_on_commit=False,
            future=True,
        )
        self.session = factory()
        self.conn = DbCompat(self.session)

    def tearDown(self):
        self.conn.close()
        self.engine.dispose()

    def test_legacy_commits_collapse_to_one_atomic_commit(self):
        commit_count = 0

        def record_commit(_session):
            nonlocal commit_count
            commit_count += 1

        event.listen(self.session, "after_commit", record_commit)
        with self.conn.transaction():
            company_id = upsert_company(self.conn, "Example", "EX")
            self.conn.commit()
            self.conn.commit()
            bulk_upsert_company_metrics(
                self.conn,
                company_id,
                annual_metrics={"revenues_annual": ("revenue", {2024: 100.0, 2025: 120.0})},
                ttm_metrics={"revenues_ttm": ("revenue", "2025-12-31", 125.0)},
                quarterly_metrics={"revenues_quarterly": ("revenue", {"2025-12-31": 35.0})},
            )

        self.assertEqual(commit_count, 1)
        rows = self.conn.execute(
            "SELECT fiscal_year, revenue FROM revenues_annual WHERE company_id = ? ORDER BY fiscal_year",
            (company_id,),
        ).fetchall()
        self.assertEqual(rows, [(2024, 100.0), (2025, 120.0)])

    def test_failed_outer_transaction_rolls_back_deferred_commits(self):
        with self.assertRaisesRegex(RuntimeError, "reject upload"):
            with self.conn.transaction():
                upsert_company(self.conn, "Rollback", "RB")
                self.conn.commit()
                raise RuntimeError("reject upload")

        count = self.conn.execute(
            "SELECT COUNT(*) FROM companies WHERE ticker = ?",
            ("RB",),
        ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_combined_dashboard_fetches_all_selected_series_in_one_shape(self):
        with self.conn.transaction():
            first = upsert_company(self.conn, "First", "ONE")
            second = upsert_company(self.conn, "Second", "TWO")
            bulk_upsert_company_metrics(
                self.conn,
                first,
                annual_metrics={
                    "revenues_annual": ("revenue", {2024: 10.0, 2025: 12.0}),
                    "op_margin_annual": ("margin", {2024: 0.1, 2025: 0.2}),
                },
                ttm_metrics={},
            )
            bulk_upsert_company_metrics(
                self.conn,
                second,
                annual_metrics={"revenues_annual": ("revenue", {2025: 20.0})},
                ttm_metrics={},
            )

        series = get_combined_dashboard_series_batch(self.conn, [first, second])
        self.assertEqual(series[first]["revenue"]["revenue"].tolist(), [10.0, 12.0])
        self.assertEqual(series[first]["margin"]["margin"].tolist(), [0.1, 0.2])
        self.assertEqual(series[second]["revenue"]["revenue"].tolist(), [20.0])
        self.assertTrue(series[second]["fcff"].empty)

    def test_optional_ttm_failure_rolls_back_before_dashboard_continues(self):
        class RecoverableConnection:
            def __init__(self):
                self.rollback_calls = 0

            def rollback(self):
                self.rollback_calls += 1

        conn = RecoverableConnection()
        annual = {2024: 10.0}
        with patch("ttc_efficiency.read_df", side_effect=RuntimeError("optional table missing")):
            merged = _merge_ttm_into_annual(
                conn,
                annual,
                "optional_metric_ttm",
                "value",
                1,
            )

        self.assertEqual(merged, annual)
        self.assertEqual(conn.rollback_calls, 1)

    def test_ttc_combined_inputs_are_loaded_in_one_query(self):
        with self.conn.transaction():
            company_id = upsert_company(self.conn, "TTC Batch", "TTCB")
            bulk_upsert_company_metrics(
                self.conn,
                company_id,
                annual_metrics={
                    "revenues_annual": ("revenue", {2024: 100.0, 2025: 110.0}),
                    "capital_expenditures_annual": ("capital_expenditures", {2025: 12.0}),
                },
                ttm_metrics={"revenues_ttm": ("revenue", "2025-12-31", 115.0)},
            )

        statement_count = 0

        def count_statement(*_args):
            nonlocal statement_count
            statement_count += 1

        event.listen(self.engine, "before_cursor_execute", count_statement)
        try:
            series = _load_ttc_combined_series_batch(self.conn, [company_id])
        finally:
            event.remove(self.engine, "before_cursor_execute", count_statement)

        self.assertEqual(statement_count, 1)
        self.assertEqual(series[company_id]["annual"]["revenue"], {2024: 100.0, 2025: 110.0})
        self.assertEqual(series[company_id]["with_ttm"]["revenue"], {2024: 100.0, 2025: 115.0})
        self.assertEqual(series[company_id]["with_ttm"]["capital_expenditures"], {2025: 12.0})

    def test_preloaded_value_creation_metrics_do_not_access_database(self):
        statement_count = 0

        def count_statement(*_args):
            nonlocal statement_count
            statement_count += 1

        event.listen(self.engine, "before_cursor_execute", count_statement)
        try:
            result = _compute_value_creation_filter_metrics(
                self.conn,
                1,
                2025,
                2020,
                {},
                {},
                preloaded_annual={},
            )
        finally:
            event.remove(self.engine, "before_cursor_execute", count_statement)

        self.assertEqual(statement_count, 0)
        self.assertTrue(all(value is None for value in result.values()))


if __name__ == "__main__":
    unittest.main()
