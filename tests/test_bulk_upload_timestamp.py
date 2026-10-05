import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import data_upload
import db_models
from core_backend import record_latest_bulk_upload, upsert_company
from db_session import DbCompat, ManagedSession


class BulkUploadTimestampTests(unittest.TestCase):
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
        self.conn = DbCompat(factory())

    def tearDown(self):
        self.conn.close()
        self.engine.dispose()

    def _status_rows(self):
        return self.conn.session.execute(
            select(db_models.company_bulk_upload_status).order_by(
                db_models.company_bulk_upload_status.c.country
            )
        ).mappings().all()

    def test_latest_timestamp_is_scoped_to_ticker_company_country_and_rolls_back(self):
        first = datetime(2026, 9, 23, 20, 0, tzinfo=timezone.utc)
        older = datetime(2026, 9, 22, 20, 0, tzinfo=timezone.utc)
        later = datetime(2026, 9, 24, 20, 0, tzinfo=timezone.utc)
        with self.conn.transaction():
            company_id = upsert_company(self.conn, "Example Ltd", "EX", country="India")
            record_latest_bulk_upload(self.conn, company_id, first)
        with self.conn.transaction():
            record_latest_bulk_upload(self.conn, company_id, older)
        self.assertEqual(len(self._status_rows()), 1)
        self.assertEqual(self._status_rows()[0]["latest_uploaded_at_utc"].day, 23)

        with self.assertRaisesRegex(RuntimeError, "failed upload"):
            with self.conn.transaction():
                record_latest_bulk_upload(self.conn, company_id, later)
                raise RuntimeError("failed upload")
        self.assertEqual(self._status_rows()[0]["latest_uploaded_at_utc"].day, 23)

        with self.conn.transaction():
            self.conn.execute("UPDATE companies SET country = ? WHERE id = ?", ("USA", company_id))
            record_latest_bulk_upload(self.conn, company_id, later)
        rows = self._status_rows()
        self.assertEqual([(row["ticker"], row["company_name"], row["country"]) for row in rows], [
            ("EX", "Example Ltd", "India"),
            ("EX", "Example Ltd", "USA"),
        ])
        self.assertEqual([row["latest_uploaded_at_utc"].day for row in rows], [23, 24])

    def test_successful_bulk_batch_records_timestamp(self):
        with tempfile.TemporaryDirectory() as folder:
            bulk_dir = Path(folder)
            file_path = bulk_dir / "EX-financials.xlsx"
            file_path.write_bytes(b"workbook")

            def ingest(_data, company, ticker, conn, country):
                return {
                    "company_id": upsert_company(conn, company, ticker, country=country),
                    "warnings": [],
                    "quarterly_upload_counts": {},
                    "business_quarter_trend_score": None,
                }

            with patch.object(data_upload, "st"), \
                 patch.object(data_upload, "_bucket_has_unlevered_beta", return_value=True), \
                 patch.object(data_upload, "ingest_financials_bytes", side_effect=ingest), \
                 patch.object(data_upload, "get_company_group_id", return_value=1), \
                 patch.object(data_upload, "add_company_group_members"), \
                 patch.object(data_upload, "_compute_bucket_dependent_metrics"):
                result = data_upload._run_bulk_upload_batch(
                    bulk_dir=bulk_dir,
                    manifest_map={"EX": {"company_name": "Example Ltd", "country": "INDIA", "industry_bucket": "Test"}},
                    financial_files=[file_path],
                    start_index=0,
                    batch_size=1,
                    conn=self.conn,
                )

        self.assertEqual(result["success_count"], 1)
        rows = self._status_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["ticker"], rows[0]["company_name"], rows[0]["country"]),
                         ("EX", "Example Ltd", "India"))
        self.assertIsNotNone(rows[0]["latest_uploaded_at_utc"])


if __name__ == "__main__":
    unittest.main()
