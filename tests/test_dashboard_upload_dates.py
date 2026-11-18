import io
import sqlite3
import unittest
from datetime import datetime

import pandas as pd
from openpyxl import load_workbook
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from combined_dashboard import _combined_dashboard_excel_bytes
from dashboard_upload_dates import UPLOAD_DATE_COLUMN, add_latest_upload_dates
from db_session import DbCompat, ManagedSession


SCHEMA = '''
CREATE TABLE companies(id INTEGER PRIMARY KEY, name TEXT, ticker TEXT, country TEXT);
CREATE TABLE company_bulk_upload_status(company_id INTEGER, company_name TEXT, ticker TEXT, country TEXT, latest_uploaded_at_utc TEXT);
INSERT INTO companies VALUES (1, 'Same Name', 'DUP', 'USA'), (2, 'Same Name', 'DUP', 'India'), (3, 'Missing', 'MISS', 'USA');
INSERT INTO company_bulk_upload_status VALUES
 (1,'Same Name','DUP','USA','2026-11-04T23:30:00-05:00'),
 (1,'Same Name','DUP','USA','2026-11-03T23:30:00Z'),
 (1,'Same Name','DUP','India','2026-12-01T10:00:00Z'),
 (1,'Old Name','DUP','USA','2026-12-02T10:00:00Z'),
 (1,'Same Name','OLD','USA','2026-12-03T10:00:00Z'),
 (2,'Same Name','DUP','India','2026-10-30T10:00:00Z');
'''


class DashboardUploadDateTests(unittest.TestCase):
    def frame(self):
        return pd.DataFrame({"_company_id": [3, 1, 2], "Ticker": ["MISS", "DUP", "DUP"], "Score": [90., 80., 70.]})

    def test_dates_preserve_score_order_and_match_full_identity_with_utc_day(self):
        with sqlite3.connect(":memory:") as conn:
            conn.executescript(SCHEMA)
            original = self.frame()
            out = add_latest_upload_dates(conn, original)
            pd.testing.assert_frame_equal(out.drop(columns=[UPLOAD_DATE_COLUMN]), original)
            self.assertNotIn(UPLOAD_DATE_COLUMN, original)
            self.assertTrue(pd.isna(out[UPLOAD_DATE_COLUMN].iloc[0]))
            self.assertEqual(out[UPLOAD_DATE_COLUMN].iloc[1], pd.Timestamp("2026-11-05"))
            self.assertEqual(out[UPLOAD_DATE_COLUMN].iloc[2], pd.Timestamp("2026-10-30"))

    def test_managed_backend_and_excel_keep_dates_typed_and_missing_dates_blank(self):
        engine = create_engine("sqlite://")
        with engine.connect() as raw:
            for statement in SCHEMA.split(";"):
                if statement.strip(): raw.exec_driver_sql(statement)
            raw.commit()
        conn = DbCompat(sessionmaker(bind=engine, class_=ManagedSession)())
        try:
            out = add_latest_upload_dates(conn, self.frame())
            payload = _combined_dashboard_excel_bytes(out.drop(columns=["_company_id"]))
            sheet = load_workbook(io.BytesIO(payload)).active
            date_index = [cell.value for cell in sheet[1]].index(UPLOAD_DATE_COLUMN) + 1
            self.assertIsNone(sheet.cell(2, date_index).value)
            self.assertEqual(sheet.cell(3, date_index).value, datetime(2026, 11, 5))
            self.assertEqual(sheet.cell(3, date_index).number_format, "dd mmm yyyy")
            self.assertEqual(sheet.freeze_panes, "A2")
        finally:
            conn.close()
            engine.dispose()

    def test_missing_upload_history_is_unavailable_without_breaking_existing_dashboards(self):
        with sqlite3.connect(":memory:") as conn:
            self.assertTrue(add_latest_upload_dates(conn, self.frame())[UPLOAD_DATE_COLUMN].isna().all())


if __name__ == "__main__":
    unittest.main()
